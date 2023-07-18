(library (dataframe assertions)
  (export
   check-index
   check-integer-gte-zero
   check-integer-positive
   check-names-unique
   check-names-symbol
   check-names
   check-names-duplicate
   check-new-names
   check-name-pairs)

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
      (assertion-violation who "names are not symbols")))

  (define (check-names names who)
    (check-names-symbol names who)
    (check-names-unique names who))

  (define (check-names-duplicate old-names new-names who)
    (unless (for-all (lambda (new-name) (not (member new-name old-names))) new-names)
      (assertion-violation who "new names duplicate existing names")))

  (define (check-new-names old-names new-names who)
    (check-names new-names who)
    (check-names-duplicate old-names new-names who))
  
  (define (check-name-pairs current-names name-pairs who)
    ;; not very thorough checking of ways a name-pair could be malformed
    (unless (for-all pair? name-pairs)
      (assertion-violation who "names not of form '(old-name new-name) ..."))
    (let ([new-names (map cadr name-pairs)])
      (check-new-names current-names new-names who)))

  )




