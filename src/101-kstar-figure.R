
# Setup ------------------------------------------------------------------------

#load libraries
library(glue)
library(tidyverse)


#load helper scripts
source("src/-Global-Parameters.R")
source("src/utils.R")

#set plot theme
EKMSW_theme <- function( ){
  theme_bw(base_family = "serif") +
    theme(legend.position = "bottom",
          legend.spacing.x = unit(2, "pt"),
          strip.background=element_blank(),
          panel.grid.minor=element_blank(),
          panel.grid.major=element_blank(),
          panel.border     = element_rect(colour = "grey60"),
          axis.text        = element_text(colour = "grey20"),
          axis.title       = element_text(colour = "grey20"),
          strip.text       = element_text(colour = "grey20"),
          legend.title     = element_text(colour = "grey20"),
          legend.text      = element_text(colour = "grey20"),
          axis.ticks = element_line(colour = "grey60")
    )
}
theme_set(EKMSW_theme())
message("changed ggplot theme")


#set specific parameters for figure

model = "KNN"

# Load data --------------------------------------------------------------------

graph_data <- read_parquet(glue("{data_path}/{model}-kstar-data.parquet"))

#rescale MAFE
graph_data <- graph_data %>%
  mutate(
    MAFE = MAFE *  100
  )


#read k-stars from file to generate labels
k_star <-  graph_data %>% 
  filter(kstar==TRUE) %>% 
  mutate(MAFE = round(MAFE, digits = 2)) %>% 
  arrange(K,-MAFE) %>% 
  mutate(x = K+((row_number())^2.8),
         xend = K,
         y= 7.57-((row_number())*.046), 
         yend = as.numeric(MAFE),
         label = paste0('k["M = ',M,'"]^"*"~',MAFE),
         nudge = 0.018
  )

#code the figure
figure <-
  graph_data %>%
  #filter(K <=200) %>% 
  filter(K %% 10 == 0) %>%
  ggplot(aes(x = K, y = MAFE, shape = factor(M), color = factor(M))) +
  geom_line() +
  geom_point() +
  scale_x_continuous(breaks = seq(20,500,20)) +
  scale_color_viridis_d() +
  guides(
    shape = guide_legend("Number of prior years (M): "),
    color = guide_legend("Number of prior years (M): ")
  ) +
  annotate("text",
           x = k_star$x,
           y = k_star$y + k_star$nudge,
           label = k_star$label, parse = TRUE,
           family = "serif"
  ) +
  annotate("segment",
           x = k_star$x, xend = k_star$xend,
           y = k_star$y, yend = k_star$yend
  ) +
  theme(legend.position = "bottom")

#preview the figure
figure

#save files
ggsave(glue("out/tune-k-m.pdf"), figure, width = 7, height = 6)
ggsave(glue("out/tune-k-m.png"), figure, width = 7, height = 6)


