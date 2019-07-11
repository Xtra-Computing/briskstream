getwd()
library(xlsx)

get_opt <- function(opt){
  lat <- as.character(opt)
  switch (lat,
          "1" = "1core",
          "2" = "2cores",
          "3" = "4cores",
          "4" = "1socket",
          "5" = "2sockets",
          "6" = "4sockets"
  )
}

workload_path<- function(app){
  lat <- as.character(app)
  switch (lat,
          "4" = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_word-count/%s_%s_%s_%s_%s", sep=""),
           "5"  = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_fraud-detection/%s_%s_%s_%s", sep=""),
           "6"  = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_log-processing/%s_%s_%s_%s", sep=""),
           "7"  = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_spike-detection/%s_%s_%s_%s_%s", sep=""),
           "8"  = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_voipstream/%s_%s_%s_%s_%s", sep=""),
           "9"  = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_traffic-monitoring/%s_%s_%s_%s", sep=""),
           "10" = paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm/%s",get_opt(opt)),"/performanceTest/output_linear-road-full/%s_%s_%s_%s_%s_%s", sep="")
  )
}

create_report<- function(app){
  lat <- as.character(app)
  switch (lat,
          "4" = "wc",
          "5" = "fd",
          "6" = "lg",
          "7" = "sd",
          "8" = "vs",
          "9" = "tm",
          "10" = "lr"
  )
}

for(opt in 3:4){
myseq<-c(1,2,4,8)
for(app in 4:10){
  execution_time<-NULL
  min_execution_time<-100000000
  argument<-NULL
  min_argument<-NULL
  s<-workload_path(app)
  for(bt in c(1)){
    if(app=="4"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          for(ct3 in myseq){
            mypath <-sprintf(s,opt,bt,ct1,ct2,ct3)
            if(file.exists(mypath)) {
              setwd(mypath)
              if(!file.exists("sink.txt")){
                file.create("sink.txt")
                write("-1","sink.txt")
              }
              current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
              current_arg<-sprintf("application:%s, opt:%s,  batch_size:%s, %s_%s_%s",app,opt,bt,ct1,ct2,ct3)
              if(min_execution_time >current_time && current_time >0){
                min_execution_time<-current_time
                min_argument<-current_arg
              }
              execution_time <-c(execution_time,current_time)
              argument<-c(argument,current_arg)
            } 
          } 
        }
      }
    }
    
    if(app=="5"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          mypath <-sprintf(s,opt,bt,ct1,ct2)
          if(file.exists(mypath)) {
            setwd(mypath)
            current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
            current_arg<-sprintf("application:%s,opt:%s, batch_size:%s, %s_%s",app,opt,bt,ct1,ct2)
            if(min_execution_time >current_time && current_time >0){
              min_execution_time<-current_time
              min_argument<-current_arg
            }
            execution_time <-c(execution_time,current_time)
            argument<-c(argument,current_arg)
          } 
        }
      }
    }
    
    if(app=="6"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          mypath <-sprintf(s,opt,bt,ct1,ct2)
          if(file.exists(mypath)) {
            setwd(mypath)
            current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
            current_arg<-sprintf("application:%s,opt:%s, batch_size:%s, %s_%s",app,opt,bt,ct1,ct2)
            if(min_execution_time >current_time && current_time >0){
              min_execution_time<-current_time
              min_argument<-current_arg
            }
            execution_time <-c(execution_time,current_time)
            argument<-c(argument,current_arg)
          } 
        }
      }
    }
    
    if(app=="7"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          for(ct3 in myseq){
            mypath <-sprintf(s,opt,bt,ct1,ct2,ct3)
            if(file.exists(mypath)) {
              setwd(mypath)
              current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
              current_arg<-sprintf("application:%s,opt:%s, batch_size:%s, %s_%s_%s",app,opt,bt,ct1,ct2,ct3)
              if(min_execution_time >current_time && current_time >0){
                min_execution_time<-current_time
                min_argument<-current_arg
              }
              execution_time <-c(execution_time,current_time)
              argument<-c(argument,current_arg)
            } 
          } 
        }
      }
    }
    
    if(app=="8"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          for(ct3 in myseq){
            mypath <-sprintf(s,opt,bt,ct1,ct2,ct3)
            if(file.exists(mypath)) {
              setwd(mypath)
              current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
              current_arg<-sprintf("application:%s,opt:%s, batch_size:%s, %s_%s_%s",app,opt,bt,ct1,ct2,ct3)
              if(min_execution_time >current_time && current_time >0){
                min_execution_time<-current_time
                min_argument<-current_arg
              }
              execution_time <-c(execution_time,current_time)
              argument<-c(argument,current_arg)
            } 
          } 
        }
      }
    }
    
    if(app=="9"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          mypath <-sprintf(s,opt,bt,ct1,ct2)
          if(file.exists(mypath)) {
            setwd(mypath)
            current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
            current_arg<-sprintf("application:%s,opt:%s, batch_size:%s, %s_%s",app,opt,bt,ct1,ct2)
            if(min_execution_time >current_time && current_time >0){
              min_execution_time<-current_time
              min_argument<-current_arg
            }
            execution_time <-c(execution_time,current_time)
            argument<-c(argument,current_arg)
          } 
        }
      }
    }
    
    if(app=="10"){
      for(ct1 in myseq){
        for(ct2 in myseq){
          for(ct3 in myseq){
            for(ct4 in myseq){
              mypath <-sprintf(s,opt,bt,ct1,ct2,ct3,ct4)
              if(file.exists(mypath)) {
                setwd(mypath)
                current_time<-scan("sink.txt",nlines=1, quiet=TRUE)
                current_arg<-sprintf("application:%s,opt:%s, batch_size:%s, %s_%s_%s_%s",app,opt,bt,ct1,ct2,ct3,ct4)
                
                if(min_execution_time >current_time && current_time >0){
                  min_execution_time<-current_time
                  min_argument<-current_arg
                }
                execution_time <-c(execution_time,current_time)
                argument<-c(argument,current_arg)
              }
            } 
          } 
        }
      }
    }
  }
  argument<-c(min_argument,argument)
  execution_time<-c(min_execution_time,execution_time)
  label<- c("Tuned","Non-Tuned", rep(NA, length(execution_time)- 2))
  
  
  setwd("C:/Users/szhang026/Documents/Profile-experiments/4-wayMachine/storm")
  execution_frame <-data.frame(label, argument,execution_time)
  colnames(execution_frame) <- c("label","argument", "execution_time")
  sname<-sprintf("%s,%s",create_report(app),get_opt(opt))
  print(sname)
  write.xlsx(execution_frame, file="storm.xlsx", sheetName=sname, append=TRUE)
  
}  
}

# plot(1:length(execution_time),execution_time,col="black",lwd=0.5,type="l",xlab="",ylab="Execution Time") # line 4..
# axis(4, at=1:length(execution_time), labels=argument, las=1,lwd = 0.5)
# 
# legend('bottomleft', # places a legend at the appropriate place 
# 
# c("Brisk.execution time"), # puts text in the legend
#  
#  lty=c(1), # gives the legend appropriate symbols (lines)
#  
#  lwd=c(2.5),col=c("black")
#  ,cex=0.5, pch=0.5,pt.cex = 0.5) # gives the legend lines the correct color and width