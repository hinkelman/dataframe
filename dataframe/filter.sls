(library (dataframe filter)
  (export dataframe-filter
          dataframe-filter*
          dataframe-filter-all
          dataframe-filter-at
          dataframe-partition
          dataframe-partition*)

  (import (rnrs)
          (only (dataframe df)
                check-dataframe
                check-df-names
                dataframe-alist
                dataframe-names
                dataframe-values-map
                make-dataframe)   
          (only (dataframe helpers)
                add-names-ls-vals
                alist-values-map
                filter-ls-vals
                flatten
                partition-ls-vals))

  ;; filter/partition ------------------------------------------------------------------------

  ;; filter-ls-vals involves zipping, filtering, and unzipping every column
  ;; alternative is to transpose to row-based,
  ;; cons bools to rows,
  ;; filter by car of rows,
  ;; cdr to remove bools,
  ;; and transpose back to column-based
  
  ;; I opted for zip/filter/unzip because the code is so simple
  ;; current approach might be more efficient when lots of rows and few columns
  ;; other approach might be more efficient when lots of columns and few rows
  ;; but I don't have a good sense of relative cost of the two approaches

  ;; wanted this syntax: (dataframe-filter* df expr)
  ;; tried to flatten the expression to extract symbols that are names
  ;; but I couldn't figure out how to unquote the extracted names
  (define-syntax dataframe-filter*
    (syntax-rules ()
      [(_ df names expr)
       (df-filter df (quote names) (lambda names expr)
                  "(dataframe-filter* df names expr)")]))

  (define (dataframe-filter df names procedure)
    (df-filter df names procedure "(dataframe-filter df names procedure)"))

  (define (df-filter df names proc who)
    (check-dataframe df who)
    (apply check-df-names df who names)
    (let* ([bools (apply map proc (dataframe-values-map df names))]
           [names (dataframe-names df)]
           [alist (dataframe-alist df)]
           [new-ls-vals (filter-ls-vals bools (map cdr alist))])
      (make-dataframe (add-names-ls-vals names new-ls-vals))))

  (define-syntax dataframe-partition*
    (syntax-rules ()
      [(_ df names expr)
       (df-partition df (quote names) (lambda names expr)
                     "(dataframe-partition* df names expr)")]))

  (define (dataframe-partition df names procedure)
    (df-partition df names procedure "(dataframe-partition df names procedure)"))

  (define (df-partition df names proc who)
    (check-dataframe df who)
    (apply check-df-names df who names)
    (let* ([bools (apply map proc (dataframe-values-map df names))]
           [names (dataframe-names df)]
           [alist (dataframe-alist df)])
      (let-values ([(keep drop) (partition-ls-vals bools (map cdr alist))])
        (values (make-dataframe (add-names-ls-vals names keep))
                (make-dataframe (add-names-ls-vals names drop))))))

  (define (dataframe-filter-all df predicate)
    (df-filter-at-all-helper df
                             predicate
                             (dataframe-names df)
                             "(dataframe-filter-all df predicate)"))

  (define (dataframe-filter-at df predicate . names)
    (df-filter-at-all-helper df
                             predicate
                             names
                             "(dataframe-filter-at df predicate names)"))

  (define (df-filter-at-all-helper df predicate names who)
    (apply check-df-names df who names)
    (let ([new-alist
           (alist-filter-at (dataframe-alist df) predicate names)])
      (if (null? (cdar new-alist))
          (assertion-violation who "Filtered dataframe is empty")
          (make-dataframe new-alist))))

  ;; convert columns in ls-vals to 0s and 1s (ls-binary) based on predicate
  ;; sum across ls-binary to create boolean list for filtering
  (define (alist-filter-at alist predicate names)
    (let* ([ls-vals (alist-values-map alist names)]
           [ls-binary (map (lambda (vals)
                             (map (lambda (val)
                                    (if (predicate val) 1 0))
                                  vals))
                           ls-vals)]
           [bools (map (lambda (x) (= x (length names)))
                       (apply map + ls-binary))])
      (add-names-ls-vals (map car alist)
                         (filter-ls-vals bools (map cdr alist)))))


  )

