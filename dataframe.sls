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
   ;; select
   dataframe-drop
   dataframe-drop*
   dataframe-select
   dataframe-select*
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
          (dataframe record-types)
          (dataframe types)
          (dataframe rename)
          (dataframe select)
          (dataframe statistics)
          (dataframe helpers))
  
  )

