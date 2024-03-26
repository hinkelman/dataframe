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
   series-type
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
   dataframe-glimpse
   ;; sort
   dataframe-sort
   dataframe-sort*
   ;; bind
   dataframe-append
   dataframe-bind
   dataframe-bind-all
   ;; modify
   dataframe-modify
   dataframe-modify*
   dataframe-modify-all
   dataframe-modify-at
   ;; aggregate
   dataframe-aggregate
   dataframe-aggregate*
   ;; join
   dataframe-left-join
   dataframe-left-join-all
   ;; reshape
   dataframe-stack
   dataframe-spread
   ;; io
   csv->dataframe
   tsv->dataframe
   dataframe->csv
   dataframe->tsv
   dataframe-read
   dataframe-write
   ;; statistics
   cumulative-sum
   sum
   product
   mean
   weighted-mean
   variance
   standard-deviation
   median
   quantile
   interquartile-range
   rle
   ;; helpers
   na?
   any-na?
   remove-na
   remove-duplicates
   rep
   transpose)

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
          (dataframe modify)
          (dataframe aggregate)
          (dataframe join)
          (dataframe reshape)
          (dataframe io)
          (dataframe statistics)
          (dataframe helpers))
  
  )

