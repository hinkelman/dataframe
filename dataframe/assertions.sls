(library (dataframe assertions)
  (export
   check-index
   check-integer-gte-zero
   check-integer-positive
   check-names-unique
   check-names-symbol
   check-names)

  (import (rnrs)
          (only (dataframe helpers)
                remove-duplicates))

  (define (check-integer-positive x x-name who)
    (unless (and (> x 0) (integer? x))
      (assertion-violation who (string-append x-name " is not a positive integer"))))
  
  (define (check-integer-gte-zero x x-name who)
    (unless (and (>= x 0) (integer? x))
      (assertion-violation who (string-append x-name " is not an integer >= 0"))))

  (define (check-index n n-max who)
    (when (> n n-max)
      (assertion-violation who (string-append "index " (number->string n) " is out of range"))))

  (define (check-names-unique names who)
    (unless (= (length names) (length (remove-duplicates names)))
      (assertion-violation who "names are not unique")))

  (define (check-names-symbol names who)
    (unless (for-all (lambda (name) (symbol? name)) names)
      (assertion-violation who "name(s) are not symbols")))

  (define (check-names names who)
    (check-names-symbol names who)
    (check-names-unique names who))
  
  )




