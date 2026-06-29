args <- commandArgs(trailingOnly = TRUE)
src <- args[[1]]
pd <- utils::getParseData(parse(src, keep.source = TRUE))
if (is.null(pd) || nrow(pd) == 0) {
  cat("line1,col1,line2,col2,id,parent,token,terminal,text\n")
} else {
  write.csv(pd, file = stdout(), row.names = FALSE)
}
