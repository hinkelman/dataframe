(library (dataframe filter)

  (export
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
   slist-ref)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe select)
                slist-select)
          (only (dataframe helpers)
                add1
                list-head
                remove-duplicates
                transpose)
          (only (dataframe assertions)
                check-integer-gte-zero
                check-integer-positive
                check-index))

  ;; procedures in this library all involve taking a subset of rows
  
  ;; head/tail ---------------------------------------------------------------------

  (define (dataframe-head df n)
    (let ([who "(dataframe-head df n)"])
      (check-dataframe df who)
      (check-integer-positive n "n" who)
      (check-index n (car (dataframe-dim df)) who))
    (make-dataframe
     (slist-head-tail
      (dataframe-names df) (dataframe-slist df) n list-head)))

  ;; dataframe-tail is based on list-tail, which does not work the same as tail in R
  (define (dataframe-tail df n)
    (let ([who  "(dataframe-tail df n)"])
      (check-dataframe df who)
      (check-integer-gte-zero n "n" who)
      (check-index (add1 n) (car (dataframe-dim df)) who))
    (make-dataframe
     (slist-head-tail
      (dataframe-names df) (dataframe-slist df) n list-tail)))

  (define (slist-head-tail names slist n proc)
    (make-slist
     names
     (map (lambda (series) (proc (series-lst series) n)) slist)))

  ;; unique ------------------------------------------------------------------------

  (define (dataframe-unique df)
    (check-dataframe df "(dataframe-unique df)")
    (let* ([names (dataframe-names df)]
           [ls-vals (map series-lst (dataframe-slist df))]
           [ls-vals-unique (transpose (remove-duplicates (transpose ls-vals)))])
      (make-dataframe (make-slist names ls-vals-unique))))

  ;; dataframe-ref ---------------------------------------------------------------
  
  (define dataframe-ref
    (case-lambda
      [(df indices) (df-ref-helper df indices (dataframe-names df))]
      [(df indices . names) (df-ref-helper df indices names)]))

  (define (df-ref-helper df indices names)
    (let ([who "(dataframe-ref df indices)"]
          [n-max (car (dataframe-dim df))])
      (apply check-df-names df who names)
      (map (lambda (n)
             (check-integer-gte-zero n "index" who)
             (check-index n n-max who))
           indices))
    (make-dataframe (slist-ref (dataframe-slist df) indices names)))

  (define (slist-ref slist indices names)
    (let* ([slist-sel (slist-select slist names)]
           [ls-vals (map series-lst slist-sel)])
      (make-slist
       names
       (map (lambda (vals)
              (map (lambda (n) (list-ref vals n)) indices))
            ls-vals))))
  
  ;; filter --------------------------------------------------------------------

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
       (df-filter df (quote names) (lambda names expr))]))

  (define (dataframe-filter df names procedure)
    (df-filter df names procedure))

  (define (df-filter df names proc)
    (let ([who "(dataframe-filter df names procedure)"])
      (check-dataframe df who)
      (apply check-df-names df who names))
    (let* ([all-names (dataframe-names df)]
           [slist (dataframe-slist df)]
           [slist-sel (slist-select slist names)]
           [bools (apply map proc (map series-lst slist-sel))]
           [new-ls-vals (filter-ls-vals bools (map series-lst slist))])
      (make-dataframe (make-slist all-names new-ls-vals))))

  (define (filter-ls-vals bools ls-vals)
    (map (lambda (vals) (filter-vals bools vals)) ls-vals))

  ;; filter vals by list of booleans of same length as vals
  (define (filter-vals bools vals)
    (let ([bools-vals (map cons bools vals)])
      (map cdr (filter (lambda (x) (car x)) bools-vals))))

  (define (dataframe-filter-all df predicate)
    (df-filter-at df predicate (dataframe-names df)))

  (define (dataframe-filter-at df predicate . names)
    (apply check-df-names df "(dataframe-filter-at df predicate names)" names)
    (df-filter-at df predicate names))

  ;; convert columns in ls-vals to 0s and 1s (ls-binary) based on predicate
  ;; sum across ls-binary to create boolean list for filtering
  (define (df-filter-at df predicate names)
    (let* ([slist (dataframe-slist df)]
           [slist-sel (slist-select slist names)]
           [ls-vals (map series-lst slist-sel)]
           [ls-binary (map (lambda (vals)
                             (map (lambda (val)
                                    (if (predicate val) 1 0))
                                  vals))
                           ls-vals)]
           [bools (map (lambda (x) (= x (length names)))
                       (apply map + ls-binary))])
      (make-dataframe
       (make-slist (dataframe-names df)
                   (filter-ls-vals bools (map series-lst slist))))))

  ;; partition --------------------------------------------------------------------

  (define-syntax dataframe-partition*
    (syntax-rules ()
      [(_ df names expr)
       (df-partition df (quote names) (lambda names expr))]))

  (define (dataframe-partition df names procedure)
    (df-partition df names procedure))

  (define (df-partition df names proc)
    (let ([who "(dataframe-partition df names procedure)"])
      (check-dataframe df who)
      (apply check-df-names df who names))
    (let* ([all-names (dataframe-names df)]
           [slist (dataframe-slist df)]
           [slist-sel (slist-select slist names)]
           [bools (apply map proc (map series-lst slist-sel))])
      (let-values ([(keep drop) (partition-ls-vals bools (map series-lst slist))])
        (values (make-dataframe (make-slist all-names keep))
                (make-dataframe (make-slist all-names drop))))))

  ;; in previous version, would pass over ls-vals twice with filter-ls-vals
  ;; (with bools negated on 1 pass)
  ;; the extra transposing in this version is faster than two passes with filter-ls-vals
  (define (partition-ls-vals bools ls-vals)
    (let loop ([bools bools]
               [ls-rows (transpose ls-vals)]
               [keep '()]
               [drop '()])
      (if (null? bools)
          (values (transpose (reverse keep)) (transpose (reverse drop)))
          (if (car bools)
              (loop (cdr bools) (cdr ls-rows) (cons (car ls-rows) keep) drop)
              (loop (cdr bools) (cdr ls-rows) keep (cons (car ls-rows) drop))))))
  
  )

