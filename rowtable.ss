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
                check-rowtable
                transpose))

  (define (dataframe->rowtable df)
    (check-dataframe df "(dataframe->rowtable df)")
    (let* ([names (dataframe-names df)]
           [ls-vals (dataframe-values-map df names)])
      (cons names (transpose ls-vals))))

  (define (rowtable->dataframe rt header?)
    (check-rowtable rt "(rowtable->dataframe rt header?)")
    (let ([names (if header?
                     (car rt)
                     (map string->symbol
                          (map string-append
                               (make-list (length (car rt)) "V")
                               (map number->string
                                    (enumerate (car rt))))))]
          [ls-vals (if header?
                       (transpose (cdr rt))
                       (transpose rt))])
      (make-dataframe (map cons names ls-vals))))

  )

