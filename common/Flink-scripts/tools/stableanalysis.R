getwd()
library(xlsx)
require(ggplot2)
require(reshape)
require(qpcR)
get_opt <- function(opt){
  y <- as.character(opt)
  switch (y, 
          "1" = "1core",
          "2" = "2cores",
          "3" = "4cores",
          "4" = "1socket",
          "5" = "2sockets",
          "6" = "4sockets",
          "7" = "batch_size",
          "8" = "hugePage"
  )
}

workload_path<- function(app){
  y <- as.character(app)
  switch (y, 
          "1" = paste("C:/Users/I309939/Documents/4wayMachineResults/Flink/stable/output_stream-greping/%s_%s_%s", sep="")
  )
}

create_report<- function(app){
  y <- as.character(app)
  switch (y, 
          "1" = "sg"
  )
}


myseq<-c(1,2,4,8,10,12,14,16,18,20,32)
for(app in 1:1){
  # List attributes.
  argument<-NULL
  Throughputs<-NULL
  AVG_Throughputs<-NULL
  Variance<-NULL
  best_argument<-NULL
  
  s<-workload_path(app)
  if(app=="1"){
    for(opt in 1:6){
      for(bt in c(1)){
        for(ct1 in myseq){
          mypath <-sprintf(s,opt,bt,ct1)
          if(file.exists(mypath)) {
            setwd(mypath)
            print("Read raw information.")
            #lines_split <- strsplit(readLines("throughput.txt"), split=":")
            Throughput <- read.table("throughput.txt")
            Latency <- 1/Throughput
            
            #Update list attributes.
            current_arg<-sprintf("%s,%s,%s,%s",app,opt,bt,ct1)
            argument<-c(argument,current_arg)
            #Throughputs<-data.frame(Throughputs,Throughput)
            AVG_Throughputs<-c(AVG_Throughputs,colMeans(Throughput))
            Variance<-c(Variance,var(Throughput))
            Throughputs<-qpcR:::cbind.na(Throughputs, Throughput)
          } 
        }
      }
    }
    Throughputs<-Throughputs[,-1]
    setwd("C:/Users/I309939/Documents/4wayMachineResults/Flink")
    colnames(Throughputs) <- argument
    AVG_Throughputs<-data.frame(t(AVG_Throughputs))
    colnames(AVG_Throughputs)<-argument
    Variance<-data.frame(t(Variance))
    colnames(Variance)<-argument
    sname<-sprintf("%s",create_report(app))
    print(AVG_Throughputs)
    print(Variance)
    plot.ts(Throughputs)
   # write.xlsx2(Throughputs, file="storm.xlsx", sheetName=sname, append=TRUE)
  }  
}