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
   ->
   ->>
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
   dataframe-series
   dataframe-values
   $
   ;; crossing
   dataframe-crossing
   ;; split
   dataframe-split
   ;; filter
   dataframe-filter
   dataframe-filter*
   dataframe-filter-all
   dataframe-filter-at
   dataframe-partition
   dataframe-partition*
   dataframe-head
   dataframe-tail
   dataframe-ref
   dataframe-unique
   ;; display
   dataframe-display
   ;; sort
   dataframe-sort
   dataframe-sort*
   ;; bind
   dataframe-append
   dataframe-bind
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
          (dataframe sort)
          (dataframe bind)
          (dataframe statistics)
          (dataframe helpers))
  
  )

