(library (dataframe)
  (export
   ;; record-types
   dataframe-slist
   dataframe-names
   dataframe-dim
   make-df*
   make-dataframe
   make-series
   make-slist
   series-name
   series-lst
   series-length
   ;; types
   count
   count-elements
   convert-type
   guess-type
   ;; rename
   dataframe-rename*
   dataframe-rename
   dataframe-rename-all
   ;; statistics
   series-sum
   series-mean
   series-min
   series-max
   rle
   ;; helpers
   na?
   remove-duplicates
   )

  (import (rnrs)
          (dataframe statistics)
          (dataframe record-types)
          (dataframe types)
          (dataframe rename)
          (dataframe helpers))
  
  )

