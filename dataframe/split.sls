(library (dataframe split)
  (export dataframe-split
          dataframe-split-helper)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe select)
                dataframe-select)
          (only (dataframe helpers)
                enumerate
                remove-duplicates
                transpose))

  ;; split ------------------------------------------------------------------------

  (define (dataframe-split df . group-names)
    (map make-dataframe
         (dataframe-split-helper
          df
          group-names
          "(datframe-split df group-names)")))

  ;; returns list of alists that are split by groups in group-names
  (define (dataframe-split-helper df group-names who)
    ;; (apply check-df-names df who group-names)
    (let* ([names (dataframe-names df)]
           [ls-vals (map series-lst (dataframe-slist df))]
           [df-select (dataframe-select df group-names)]
           [ls-vals-select (map series-lst (dataframe-slist df-select))]
           [ls-rows (transpose ls-vals)]
           [ls-rows-select (transpose ls-vals-select)]
           [ls-rows-grp (remove-duplicates ls-rows-select)]
           [grp-indexes (enumerate ls-rows-grp)]
           [ls-rows-indexed (index-ls-rows ls-rows ls-rows-select ls-rows-grp grp-indexes)]
           [ls-rows-indexed-split (split-ls-rows ls-rows-indexed grp-indexes)])
      (map (lambda (x) (ls-rows-indexed->slist x names)) ls-rows-indexed-split)))
  
  (define (index-ls-rows ls-rows ls-rows-select ls-rows-grp grp-indexes)
    (map (lambda (ls-row ls-row-select)
           (let* ([bool-index (map (lambda (ls-row-grp grp-index)
                                     (cons (equal? ls-row-select ls-row-grp) grp-index))
                                   ls-rows-grp grp-indexes)]
                  ;; this should filter down to a list of one element
                  [i (cdar (filter (lambda (x) (car x)) bool-index))])
             (cons i ls-row)))
         ls-rows ls-rows-select))

  (define (split-ls-rows ls-rows-indexed grp-indexes)
    (map (lambda (index)
           (filter (lambda (ls-row) (= (car ls-row) index)) ls-rows-indexed))
         grp-indexes))

  (define (ls-rows-indexed->slist ls-rows-indexed names)
    (let ([ls-rows (map cdr ls-rows-indexed)])
      (make-slist names (transpose ls-rows))))
  
  )

