library(stringr)
library(dplyr)
filenames = list.files('./movie-scores/dat')
datfiles = filenames[str_detect(filenames, '\\.dat')]
for(f in datfiles) {
  df = read.delim(sprintf('./movie-scores/dat/%s', f), sep = '\t', as.is = TRUE)
  if(f == 'movies.dat') df = df %>% group_by(imdbID) %>% filter(row_number() == 1)
  f.tsv = str_replace(f, '\\.dat', '\\.tsv')
  write.table(df, file = sprintf('./movie-scores/tsv/%s', f.tsv), row.names = FALSE, na = "", quote = FALSE, sep = '\t')
}
