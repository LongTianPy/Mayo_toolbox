# Title     : Methylation analysis
# Objective : Run PCA analysis over each file to see if tumor can be split from normal
# Created by: longtian
# Created on: 7/20/18

setwd("/data2/external_data/Sun_Zhifu_zxs01/summerprojects/ltian/MethylDB_essentials/filtered_data")
library(ggplot2)
library(grid)
library(gridExtra)
data_files <- list.files("./")

cancer_type_analysis <- function(x){
  df <- read.table(x,header=TRUE,sep="\t")
  data <- df[,4:]
  PC <- prcomp(data)
  PCi <- data.frame(PC$x,Status=df$Status)
  p <- ggplot(PCi,aes(x=PC1,y=PC2,col=Status))+
    geom_point(size=3,alpha=0.5)+ #Size and alpha just for fun
    scale_color_manual(values = c("#FF1BB3","#A7FF5B","#99554D"))+ #your colors here
    theme_classic()
  ggsave(paste(x,"pdf",sep="."),p)
}

for(i in data_files){
  cancer_type_analysis(i)
}
