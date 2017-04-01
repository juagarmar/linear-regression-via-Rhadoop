#Set up the enviroment#
Sys.setenv(HADOOP_CMD='/usr/bin/hadoop')
Sys.setenv(HADOOP_HOME='/usr/lib/hadoop-0.20-mapreduce')
Sys.setenv(HADOOP_STREAMING='/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.6.0-mr1-cdh5.7.1.jar')
library(rJava)
library(rmr2)
library(rhdfs)
hdfs.init()

#Define the arguments 'x' & 'y'#
table<-read.csv('http://archive.ics.uci.edu/ml/machine-learning-databases/00265/CASP.csv', sep=",")
table<-as.numeric(unlist(table))
table<-matrix(table, ncol=10)
X1<-to.dfs(table)

#1st map-reduce to calculate t(X)*X#
mapper=function(.,Xr){
  Xr<-Xr[,-1]
  keyval(1,list(t(Xr)%*%Xr))}
  
#reduce function sums a list of matrices#
reducer=function(.,A){
  keyval(1,list(Reduce('+',A)))}

#2nd map-reduce to calculate t(X)*y#
mapper2=function(.,Xr){
  yr<-Xr[,1]
  Xr<-Xr[,-1]
  keyval(1,list(t(Xr)%*%yr))}
  
#Calculate t(X)*X#
XtX<-values(
  from.dfs(
    mapreduce(
      input=X1,
       map=mapper,
      reduce=reducer,
      combine=T)))[[1]]
      
#Calculate t(X)*y#
Xty<-values(
  from.dfs(
    mapreduce(
      input=X1
      , map=mapper2,
      reduce=reducer,
      combine=T)))[[1]]
      
#Solution#
beta<-solve(XtX, Xty)
beta
