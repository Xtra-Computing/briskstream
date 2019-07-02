##method 1 flink-application
start_fun=0
end_fun=20
for(app in c(4:10)){
  for(fnt in start_fun:end_fun){
    s<-paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/flink-tracing/executionNode/genlib/%s/app_function_gaps%s",app,fnt),".txt", sep="")
    print(s)
    res <- executionNode(scan(s))
    #length(res)
    #F10 <-ecdf(res)
    #summary(F10)
    
    ecdf1 <- ecdf(res)
    print(res)
    if(fnt==start_fun){
      if(app==4)
        plot(ecdf1, main="user-application methods footprint",xlab="executionNode-footprint (KB)",
             ylab = "CDF", verticals=TRUE, do.points=FALSE,xlim=c(0,20), ylim=c(0,1), col=1)
      if(app==5)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,10000000), ylim=c(0,1), col=2,add=TRUE)
      if(app==6)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=3,add=TRUE)
      if(app==7)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=4,add=TRUE)
      if(app==8)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=5,add=TRUE)
      if(app==9)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=6,add=TRUE)
      if(app==10)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=7,add=TRUE)
      
      abline(v=executionNode(32000),col="black")
    }
    
    else{
      if(app==4)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,50000), ylim=c(0,1), col=1,add=TRUE)
      if(app==5)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,10000000), ylim=c(0,1), col=2,add=TRUE)
      if(app==6)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=3,add=TRUE)
      if(app==7)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=4,add=TRUE)
      if(app==8)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=5,add=TRUE)
      if(app==9)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=6,add=TRUE)
      if(app==10)
        plot(ecdf1, verticals=TRUE, do.points=FALSE,xlim=c(0,200000), ylim=c(0,1), col=7,add=TRUE)
    }
  }
}
legend("topleft", legend = c("wc","fd","lg","sd","vs","tm","lr"), col=1:7, pch=1) # optional legend


##method 2 all cdf seperate
start_fun=0
end_fun=2000
for(app in c(4:10)){
  for(fnt in start_fun:end_fun){
    #s<-paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/flink-tracing/executionNode/genlib/%s/storm_function_gaps%s",app,fnt),".txt", sep="")
    s<-paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/flink-tracing/executionNode/genlib/%s/gaps%s",app,fnt),".txt", sep="")
    #s<-paste(sprintf("C:/Users/szhang026/Documents/Profile-experiments/flink-tracing/executionNode/genlib/%s/app_function_gaps%s",app,fnt),".txt", sep="")
    print(s)
    res <- log10(scan(s))
    
    #length(res)
    #F10 <-ecdf(res)
    #summary(F10)
    
    ecdf1 <- ecdf(res)
    
    if(fnt==start_fun){
      if(app==4){
        #jpeg('word-count.jpg')
        plot(ecdf1, main="word-count",xlab="executionNode-footprint (KB)", xlim=c(0,10), ylab = "cdf", col='red')
      }
      if(app==5){
        #jpeg('fraud-detection.jpg')
        plot(ecdf1, main="fraud-detection",xlab="executionNode-footprint (KB)", xlim=c(0,20), ylab = "cdf",  col='blue')
      }        
      if(app==6){
        #jpeg('executionNode-processing.jpg')
        plot(ecdf1, main="executionNode-processing",xlab="executionNode-footprint (KB)", xlim=c(0,20), ylab = "cdf", col='yellow')
      }
      if(app==7){
        #jpeg('spike-detection.jpg')
        plot(ecdf1, main="spike-detection",xlab="executionNode-footprint (KB)", xlim=c(0,20), ylab = "cdf", col='green')
      }
      if(app==8){
        #jpeg('voipstream.jpg')
        plot(ecdf1, main="voipstream",xlab="executionNode-footprint (KB)", xlim=c(0,20), ylab = "cdf", col='orange')
      }
      if(app==9){
        #jpeg('traffic-monitoring.jpg')
        plot(ecdf1, main="traffic-monitoring",xlab="executionNode-footprint (KB)", xlim=c(0,20), ylab = "cdf", col='brown')
      }
      if(app==10){
        #jpeg('linear-road-full.jpg')
        plot(ecdf1, main="linear-road-full",xlab="executionNode-footprint (KB)", xlim=c(0,20), ylab = "cdf", col='gray')
      }
      abline(v=log10(32000),lwd=5,col="black")
    }
    
    else{
      if(app==4)
        lines(ecdf1,  col='red')
      if(app==5)
        lines(ecdf1,  col='blue')
      if(app==6)
        lines(ecdf1,  col='yellow')
      if(app==7)
        lines(ecdf1,  col='green')
      if(app==8)
        lines(ecdf1,  col='orange')
      if(app==9)
        lines(ecdf1,  col='brown')
      if(app==10)
        lines(ecdf1,  col='gray')
    }
  }
  #dev.off()
}
