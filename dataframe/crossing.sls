(library (dataframe crossing)
  (export )

  (import (rnrs)
          (dataframe record-types))

  ;; crossing/cartesian-product ----------------------------------------------------------------

  (define (dataframe-crossing . obj)
    ;; doesn't currently have very informative error messages
    (let* ([names (flatten (map (lambda (x) (if (dataframe? x)
                                                (dataframe-names x)
                                                (car x)))
                                obj))]
           ;; transpose the col-oriented alist to keep rows as a unit in the cartesian product
           [lst (map (lambda (x) (if (dataframe? x)
                                     (transpose (map cdr (dataframe-alist x)))
                                     (cdr x)))
                     obj)]
           [ls-vals (transpose (map flatten (apply cartesian-product lst)))])
      (make-dataframe (add-names-ls-vals names ls-vals))))
  )

