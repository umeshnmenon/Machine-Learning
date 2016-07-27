###################################################################################
#This is a auto installer script which takes a folder name as input and loads all the 
#.R files and checks the required packages and installs them if they are not available
###################################################################################


folder_path="<<GIVE YOUR FOLDER PATH>>" # and then source the file


installPackages<-function(folder_path){
  filenames <- list.files(folder_path, pattern="*.R", full.names=TRUE)
  libs <- lapply(filenames, FUN=function(x) {
    contents<-read.table(x, sep="\n")
    libs<-unlist(sapply(contents$V1, FUN=addLibTree))
    libs<-libs[!is.na(libs)]
    libs <- libs[!(libs %in% installed.packages()[,"Package"])]
    if(length(libs)) install.packages(libs)
  })
}

addLibTree<-function(x){
  stmnt<-as.character(x)
  ch<-grep("library|require", stmnt)
  libs<-NA
  if (length(ch)>0){
    ch2<-grep(";", stmnt)
    if (length(ch2)>0){
      stmnts<-unlist(strsplit(as.character(stmnt), ";", fixed=TRUE))
      for(i in 1:length(stmnts)){
        libs<-c(libs, do.call(rbind, as.list(addLibTree(as.character(stmnts[i])))))
      }
    }else{
      m<-regexec("[library|require]\\((.*?)\\)", as.character(stmnt))
      libs<-do.call(rbind, lapply(regmatches(stmnt, m), `[`, c(2L)))
    }
  }
  return(libs)
}


installPackages(folder_path)

