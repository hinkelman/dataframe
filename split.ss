(library (dataframe split)
  (export dataframe-split
          dataframe-split-helper)

  (import (chezscheme)
          (only (dataframe df)
                check-df-names
                dataframe-alist
                make-dataframe)   
          (only (dataframe helpers)
                add-names-ls-vals
                alist-select
                partition-ls-vals
                transpose
                unique-rows))

  ;; split ------------------------------------------------------------------------

  (define (dataframe-split df . group-names)
    (dataframe-split-helper df group-names #f))

  ;; also used in dataframe-aggregate
  (define (dataframe-split-helper df group-names return-groups?)
    (apply check-df-names df "(dataframe-split df group-names)" group-names)
    (let-values ([(alists groups) (alist-split (dataframe-alist df) group-names #t)])
      (let ([dfs (map make-dataframe alists)])
        (if return-groups?
            (values dfs groups)
            dfs))))

  ;; returns two values
  ;; first value is list of alists representing all columns in the dataframe
  ;; second value is a list of alists representing the grouping columns in the dataframe
  (define (alist-split alist group-names return-groups?)
    (let* ([ls-vals-select (map cdr (alist-select alist group-names))]
           [group-vals (unique-rows ls-vals-select #t)])
      (let-values ([(alists groups)
                    (alist-split-partition-loop alist group-names group-vals '() '())])
        (if return-groups?
            (values alists groups)
            alists))))

  ;; group-vals is a row-based list of unique grouping combinations
  ;; groups-out is a list of the column-based lists (i.e., alist) of unique grouping combinations
  (define (alist-split-partition-loop alist group-names group-vals alists-out groups-out)
    (cond [(null? group-vals)
           (values (reverse alists-out)
                   (reverse groups-out))]
          [else
           ;; group-vals-row is a single row of group-vals representing one unique grouping combination
           (let ([group-vals-row (car group-vals)]) 
             (let-values ([(keep drop) (alist-split-partition alist group-names group-vals-row)])
               (alist-split-partition-loop drop
                                           group-names
                                           (cdr group-vals)
                                           (cons keep alists-out)
                                           (cons (add-names-ls-vals
                                                  group-names
                                                  (transpose (list group-vals-row)))
                                                 groups-out))))]))
  
  (define (alist-split-partition alist group-names group-vals)
    (let ([names (map car alist)]
          [ls-vals (map cdr alist)]
          [bools (map-equal-ls-vals
                  group-vals
                  (map cdr (alist-select alist group-names)))])
      (let-values ([(keep drop) (partition-ls-vals bools ls-vals)])
        (values (add-names-ls-vals names keep)
                (add-names-ls-vals names drop)))))

  ;; objective is to identify rows from ls-vals where
  ;; value in every column matches comparison value
  ;; bools in the let are 0s and 1s (not #f and #t)
  (define (map-equal-ls-vals comp-vals ls-vals)
    (let ([num-cols (length ls-vals)]
          [bools (map (lambda (comp-val vals) (map-equal comp-val vals))
                      comp-vals
                      ls-vals)])
      (map (lambda (sum) (= num-cols sum)) (apply map + bools))))
  
  ;; returns list of 0s and 1s (#f and #t) of same length as vals
  ;; boolean list used to identify rows that are equal to comparison value, comp-val
  (define (map-equal comp-val vals)
    (let ([pred (cond
                 [(number? comp-val) =]
                 [(string? comp-val) string=?]
                 [(symbol? comp-val) symbol=?]
                 [else equal?])])
      (map (lambda (val) (if (pred comp-val val) 1 0)) vals)))

  )

