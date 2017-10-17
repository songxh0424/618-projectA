library(stringr)
filenames = list.files('./movie-scores')
datfiles = filenames[str_detect(filenames, '\\.dat')]
for(f in datfiles) {
  df = read.delim(sprintf('./movie-scores/%s', f), sep = '\t', as.is = TRUE)
  f.csv = str_replace(f, '\\.dat', '\\.csv')
  write.csv(df, file = sprintf('./movie-scores/%s', f.csv), row.names = FALSE, na = "")
}
