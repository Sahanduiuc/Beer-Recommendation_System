## Libraries
library(readr)
library(dplyr)
library(Matrix)
library(irlba)
library(lsa)
library(doParallel)
library(parallel)
library(foreach)
library(purrr)
library(sparklyr)

# Configuring Spark environment  
Sys.setenv(JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home")
conf <- spark_config()
conf$`sparklyr.shell.driver-memory` <- "20G"  
conf$spark.memory.fraction <- 0.6
sc <- spark_connect(master = "local", config = conf, version = "2.1.0")

beer_data <- spark_read_csv(sc, "beer_data_fin.csv")
head(beer_data)

# Find number of users in the data 
num_users <- beer_data %>% group_by(user_id) %>% summarise(count = n()) %>%
             sdf_nrow

i <- beer_data %>% 
     # Find number of ratings for each user and sort by user_id
     group_by(user_id) %>% summarise(count = n()) %>% arrange(user_id) %>% 
     # Convert from Spark dataframe to tibble and extract
     # count (number of ratings) vector
     select(count) %>% collect %>% .[["count"]] 

# Repeat user_id by the number of ratings associated with each user
i <- rep(1:num_users, i)

# Creating Spark dataframe with ids for each beer 
beer_key <- beer_data %>% distinct(beer_full) %>% sdf_with_sequential_id

# Merging unique beer ids to the beer data with left_join 
j <- left_join(beer_data, beer_key, by = "beer_full") %>%
     # Grouping by user_id, nesting beer_ids in user_ids, and sorting by user_id
     group_by(user_id) %>% summarise(user_beers = collect_list(id)) %>%
     arrange(user_id) %>% 
     # Unnesting beer ids (with explode), bringing data into R,
     # and extracting column vector 
     select(user_beers) %>% mutate(vec = explode(user_beers)) %>% select(vec) %>%
     collect %>% .[["vec"]]

# Turning beer key (beers by unique id) from Spark dataframe to regular dataframe
beer_key <- beer_key %>% collect

# Sort data by user_id, bring data into R, and extract user_score vector 
x <- beer_data %>% arrange(user_id) %>% select(user_score) %>% collect %>%
     .[["user_score"]]

# Creating sparse matrix of beer ratings
beer_sparse <- sparseMatrix(i = i, j = j, x = x)

head(beer_sparse)

beer_sparse_svd <- irlba(beer_sparse, 25)

head(beer_sparse_svd$v)


 # Setting up and registering a cluster for parallel processing #
 cl <- makeCluster(detectCores() - 1)
 registerDoParallel(cl)
 
 # Setting up the foreach loop (this takes time to run)
 item_similarity_matrix <- foreach(i = 1:nrow(beer_key),
                              .packages = c("dplyr", "Matrix", "lsa")) %dopar% {
 
   # Calculating the cosine distances between a given beer (i) and all the
   # beers in the sparse matrix
   sims <- cosine(t(beer_sparse_svd$v)[,i], t(beer_sparse_svd$v))
 
   # Finding arrange the cosine distances in descending order,
   # finding the 501th biggest one
   cutoff <- sims %>% tibble %>% arrange(desc(.)) %>% .[501,] %>% .[["."]]
 
   # Limiting the beer_key dataframe to beers with large enough
   # similarity scores
   sims.test <- beer_key %>% .[which(sims >= cutoff & sims < 1),]
 
   # Appending similarity scores to the abridged dataframe and sorting by
   # similarity score
   sims.test <- sims.test %>% mutate(score = sims[sims >= cutoff & sims < 1]) %>%
     arrange(desc(score))
 
   # Changing column names of the final tibble
   names(sims.test) <- c(beer_key[i,] %>% .[["beer_full"]], "id", "score")
 
   return(sims.test)
 }


# Searching for Sculpin in the beer_key
grep("Sculpin", beer_key$beer_full, value = TRUE)

# Beers similar to Sculpin 
item_similarity_matrix[grep("Sculpin", beer_key$beer_full)[6]]

set.seed(123)
item_similarity_matrix[base::sample(nrow(beer_key), 1)]

# Creating a 24542-length vector of beer ratings for user 3
example_user <- beer_sparse[3,]

rated_beer_ids <- which(example_user != 0)

sim_scores <- map(item_similarity_matrix, ~.x %>%
                    filter(id %in% rated_beer_ids) %>%
                    .[["score"]])


candidate_beer_ids <- which(sim_scores %>% map(., ~length(.x) >= 5) %>% unlist)


candidate_beer_ids <- candidate_beer_ids[!(candidate_beer_ids %in%
                                             rated_beer_ids)]

# Number of candidate beers 
length(candidate_beer_ids)


denoms <- map(item_similarity_matrix[candidate_beer_ids], ~.x %>%
                filter(id %in% rated_beer_ids) %>% .[["score"]] %>% sum)

# List of similarity scores 
sims_vecs <- map(item_similarity_matrix[candidate_beer_ids],
                 ~.x %>% filter(id %in% rated_beer_ids) %>% .[["score"]])

# List of ratings 
ratings_vecs <- map(item_similarity_matrix[candidate_beer_ids], 
                     ~example_user[.x %>% filter(id %in% rated_beer_ids) %>%
                                     .[["id"]]])

nums <- map2(sims_vecs, ratings_vecs, ~sum(.x*.y))

predicted_ratings <- map2(nums, denoms, ~.x/.y)

pred_ratings_tbl <- tibble(beer_full = beer_key %>% 
                          filter(id %in% candidate_beer_ids) %>% .[["beer_full"]], 
                          pred_rating = predicted_ratings %>% unlist) %>%
                          arrange(desc(pred_rating))
head(pred_ratings_tbl)


tibble(beer_full = beer_key[which(example_user != 0),] %>% .[["beer_full"]], 
       rating = example_user[which(example_user != 0)]) %>%
       arrange(desc(rating)) %>% head



recommend_beers <- function(input_vec, similarity_cutoff = 3){
  
# Replace missing values with 0
input_vec[is.na(input_vec)] <- 0 
  
if(length(input_vec) != nrow(beer_key)){stop("Please enter a 24502-length vector!")}else if(
  length(test_vec[test_vec > 5 | test_vec < 0]) > 0){stop("Vector can only contain values between 0 and 5!")}
  
rated_beer_ids <- which(input_vec != 0)

sim_scores <- map(item_similarity_matrix, ~.x %>%
                    filter(id %in% rated_beer_ids) %>%
                    .[["score"]])

candidate_beer_ids <- which(sim_scores %>% map(., ~length(.x) >= similarity_cutoff) %>% unlist)

if(!is_empty(candidate_beer_ids)){

candidate_beer_ids <- candidate_beer_ids[!(candidate_beer_ids %in%
                                             rated_beer_ids)]

denoms <- map(item_similarity_matrix[candidate_beer_ids], ~.x %>%
                filter(id %in% rated_beer_ids) %>% .[["score"]] %>% sum)

# List of similarity scores 
sims_vecs <- map(item_similarity_matrix[candidate_beer_ids],
                 ~.x %>% filter(id %in% rated_beer_ids) %>% .[["score"]])

# List of ratings 
ratings_vecs <- map(item_similarity_matrix[candidate_beer_ids], 
                    ~input_vec[.x %>% filter(id %in% rated_beer_ids) %>%
                                    .[["id"]]])

nums <- map2(sims_vecs, ratings_vecs, ~sum(.x*.y))

predicted_ratings <- map2(nums, denoms, ~.x/.y)

pred_ratings_tbl <- tibble(beer_full = beer_key %>% 
                             filter(id %in% candidate_beer_ids) %>% .[["beer_full"]], 
                           pred_rating = predicted_ratings %>% unlist) %>%
  arrange(desc(pred_rating))

head(pred_ratings_tbl) %>% return}else{print("You haven't rated enough beers!")}
}



