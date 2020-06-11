;; rowtable is a bad name to describe list of rows; as used in read-csv and write-csv in (chez-stats csv)

(library (dataframe rowtable)
  (export
   dataframe->rowtable
   rowtable->dataframe)

  (import (chezscheme)
          (only (dataframe df)
                check-dataframe
                dataframe-names
                dataframe-values-map
                make-dataframe)
          (only (dataframe helpers)
                transpose))

  (define (dataframe->rowtable df)
    (check-dataframe df "(dataframe->rowtable df)")
    (let* ([names (dataframe-names df)]
           [ls-vals (dataframe-values-map df names)])
      (cons names (transpose ls-vals))))

  (define (rowtable->dataframe rt header?)
    (unless (and (list? rt)
                 (apply = (map length rt)))
      (assertion-violation "(rowtable->dataframe rt header?)"
			   "rt is not a rowtable"))
    (let ([names (if header?
                     (convert-header (car rt))
		     (make-header (length (car rt))))]
          [ls-vals (if header?
                       (transpose (cdr rt))
                       (transpose rt))])
      (make-dataframe (map cons names ls-vals))))

  (define (make-header n)
    (map string->symbol
	 (map string-append
	      (make-list n "V")
	      (map number->string (iota n)))))

  (define (convert-header ls)
    (unless (for-all (lambda (x) (or (string? x) (symbol? x))) ls)
      (assertion-violation "(rowtable->dataframe rt header?)"
			   "header row must be comprised of strings or symbols"))
    (map (lambda (x) (if (symbol? x) x (string->symbol x))) ls))

  )

