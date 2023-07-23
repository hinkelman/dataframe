(library (dataframe)
  (export
   ;; record-types
   dataframe-contains?
   dataframe?
   dataframe-equal?
   dataframe-slist
   dataframe-names
   dataframe-dim
   make-df*
   make-dataframe
   make-series*
   make-series
   make-slist
   series?
   series-equal?
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
   ;; crossing
   dataframe-crossing
   ;; split
   dataframe-split
   ;; filter
   dataframe-head
   dataframe-tail
   dataframe-ref
   dataframe-unique
   ;; display
   dataframe-display
   ;; statistics
   series-sum
   series-mean
   series-min
   series-max
   rle
   ;; helpers
   na?
   remove-duplicates)

  (import (rnrs)
          (dataframe record-types)
          (dataframe types)
          (dataframe rename)
          (dataframe select)
          (dataframe crossing)
          (dataframe split)
          (dataframe filter)
          (dataframe display)
          (dataframe statistics)
          (dataframe helpers))
  
  )

