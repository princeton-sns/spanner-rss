#!/usr/bin/env Rscript
library("argparse")
library("dplyr")
library("forcats")
library("ggplot2")
library("ggsci")
library("readr")
library("scales")
library("tidyr")

parser <- ArgumentParser()

parser$add_argument("-o", "--out", type = "character", help = "out file for plot")
parser$add_argument("csvs",
  metavar = "csv", type = "character", nargs = "+",
  help = "list of csv files to read in"
)

args <- parser$parse_args()

out <- args$out

data <- tibble()
i <- 0
for (csv in args$csvs) {
  d <- mutate(read_csv(csv, col_names = c("latency", "percentile")), protocol = i)
  data <- bind_rows(data, d)
  i <- i + 1
}

data

data <- data %>%
    mutate(
        protocol = case_when(
            protocol == 0 ~ "Spanner",
            protocol == 1 ~ "Spanner-VSS"
        ),
        protocol = fct_relevel(protocol, c("Spanner", "Spanner-VSS"))
    )

data

tail_trans = function() trans_new("tail", function(x) -log10(1 - x), function(x) 1 - 10^(-x))

p <- ggplot(
    data,
    aes(
        x = latency,
        y = percentile,
        color = protocol
    )) +
    geom_line(size = 2) +
    scale_x_continuous(
        limits = c(0, 465),
        labels = comma,
        breaks = pretty_breaks(n = 10),
        expand = c(0, 0),
    ) +
    scale_y_continuous(
        limits = c(0, 0.99991),
        trans = "tail",
        labels = c(0, 0.9, 0.99, 0.999, 0.9999),
        breaks = c(0, 0.9, 0.99, 0.999, 0.9999),
        expand = c(0, 0),
    ) +
    scale_color_brewer(type = "div", palette = "Paired") +
    labs(
        x = "Latency (ms)",
        y = "Fraction of RO Transactions",
        color = ""
    ) +
    theme_classic(
        base_size = 28,
        base_family = "serif"
    ) +
    theme(
        axis.text = element_text(size = 28, color = "black"),
        axis.title = element_text(size = 32, color = "black"),
        legend.text = element_text(size = 28, color = "black"),
        legend.title = element_text(size = 32, color = "black"),
        legend.position = "top",
        legend.justification = "left",
        legend.margin = margin(0, 0, -10, 0),
        panel.grid.major.x = element_line(color = "grey", linetype = 2),
        panel.grid.major.y = element_line(color = "grey", linetype = 2),
        panel.spacing = unit(0, "mm"),
        plot.margin = margin(5, 5, 10, 5),
        )

width <- 10 # inches
height <- (3 / 4) * width

ggsave(out, plot = p, height = height, width = width, units = "in")
