(library (dataframe crossing)
  (export dataframe-crossing)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                transpose
                flatten))


  ;; obj must be a series or dataframe
  (define (dataframe-crossing . obj)
    (let* ([names
            (flatten (map (lambda (x) (if (dataframe? x)
                                          (dataframe-names x)
                                          (series-name x)))
                          obj))]
           ;; transpose the col-oriented alist to keep
           ;; rows as a unit in the cartesian product
           [lst (map (lambda (x) (if (dataframe? x)
                                     (transpose (map series-lst (dataframe-slist x)))
                                     (series-lst x)))
                     obj)]
           [ls-vals (transpose (map flatten (apply cartesian-product lst)))])
      (make-dataframe (make-slist names ls-vals))))

  (define (cartesian-product . lst)
    (fold-right product-of-two '(()) lst))

  (define (product-of-two lst1 lst2)
    (apply append
           (map (lambda (x)
                  (map (lambda (y)
                         (cons x y))
                       lst2))
                lst1)))

  
  )

