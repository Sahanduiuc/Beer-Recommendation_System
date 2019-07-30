library(highcharter)

hcoptslang <- getOption("highcharter.lang")
hcoptslang$thousandsSep <- ","
options(highcharter.lang = hcoptslang)


thm <- hc_theme_merge(
  hc_theme_tufte(),
  hc_theme(
    colors = c(hex_to_rgba("#f0027f", ".5"), 
               hex_to_rgba("#fdc086", ".5"), 
               hex_to_rgba("#7fc97f", ".5"),
               hex_to_rgba("#f0f078", ".5"), 
               hex_to_rgba("#beaed4", ".5"), 
               hex_to_rgba("#386cb0", ".4")),
    chart = list(
      backgroundColor = "#fbfbfb"),
    legend = list(
      itemStyle = list(
        fontFamily = 'Andale Mono',
        color = 'black'
      )),
    title = list(
      style = list(
        color = '#333333',
        fontFamily = "Optima"
      )
    ),
    subtitle = list(
      style = list(
        color = '#666666',
        fontFamily = "Georgia"
      )
    )
  ))

hchart(beer_data_fin_ab %>% mutate(ave_score = as.numeric(ave_score),
                                   num_reviews = as.integer(num_reviews)), "scatter", 
       hcaes(x = ave_score, y = num_reviews, group = style2)) %>% 
  hc_tooltip(headerFormat = '',
             pointFormat = "<b>Beer</b>: {point.beer_fin}  <br> <b>Reviews</b>: {point.y} <br> <b>Rating</b>: {point.x}") %>%
  hc_xAxis(title = list(text = ""), min = 1.5, max = 5) %>% 
  hc_yAxis(title = list(text = ""), min = 0, max = 35000) %>% 
  hc_title(text = "Beers of America") %>%
  hc_subtitle(text = "By rating and style") %>% 
  hc_add_theme(thm) %>% hc_boost(seriesThreshold = 1)
