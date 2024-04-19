(library (dataframe sort)
  (export dataframe-sort
          dataframe-sort*)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe select)
                slist-select)
          (only (dataframe helpers)
                enumerate
                remove-duplicates))

  ;; sort ------------------------------------------------------------------------

  ;; as a reminder for myself...
  ;; < as a sort predicate is interpreted as increasing
  ;; i.e., thing on left is smaller than thing on right

  ;; like filter/partition, sort works on columns
  ;; might be less efficient on dataframes with many columns
  ;; than a row-based approach

  (define-syntax dataframe-sort*
    (syntax-rules ()
      [(_ df (predicate name) ...)
       (df-sort df (list predicate ...) (list (quote name) ...))]))

  (define (dataframe-sort df predicates names)
    (df-sort df predicates names))

  (define (df-sort df predicates names)
    (check-dataframe df "(dataframe-sort df predicates names)")
    (let* ([slist (dataframe-slist df)]
           [slist-sel (slist-select (dataframe-slist df) names)]
           [all-names (dataframe-names df)]
           [ranks (sum-row-ranks slist-sel predicates)]
           [ls-vals-sorted (sort-ls-vals ranks (map series-lst slist))])
      (make-dataframe (make-slist all-names ls-vals-sorted))))

  ;; returns the weighted sum of the ranks for each row
  (define (sum-row-ranks slist predicates)
    (let* ([ls-vals (map series-lst slist)]
           [weights (calc-weights ls-vals)]
           [ls-ranks (map (lambda (predicate ls weight)
                            (rank-list predicate ls weight))
                          predicates ls-vals weights)])
      (apply map + ls-ranks)))

  (define (sort-ls-vals ranks ls-vals)
    (map (lambda (vals) (sort-vals ranks vals)) ls-vals))

  ;; calculate weights for each column
  ;; logic is to start with arbitrary weight for first column
  ;; and the weights for subsequent columns are the weight from
  ;; the previous column divided by the length of unique values in the current column
  ;; my initial solution didn't anticipate the problem of very large numbers of unique values
  (define (calc-weights ls-vals)
    (define (loop lengths results)
      (if (null? lengths)
          (reverse results)
          (loop (cdr lengths)
                (cons (/ (car results) (car lengths))
                      results))))
    (let* ([unique-vals (map remove-duplicates ls-vals)]
           [unique-lengths (map length unique-vals)])
      (loop (cdr unique-lengths) '(1000))))

  ;; returns list of weighted rank values for every value in ls 
  (define (rank-list predicate ls weight)
    (let* ([unique-sorted (list-sort predicate (remove-duplicates ls))]
           [ranks (map (lambda (x) (* x weight)) (enumerate unique-sorted))]
           [lookup (map cons unique-sorted ranks)]) 
      (map (lambda (x) (cdr (assoc x lookup))) ls)))

  ;; sort list of values in increasing order by ranks
  (define (sort-vals ranks vals)
    (let ([ranks-vals (map cons ranks vals)])
      (map cdr (list-sort (lambda (x y) (< (car x) (car y))) ranks-vals))))
  
  )

