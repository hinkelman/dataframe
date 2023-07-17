(library (dataframe statistics)
  (export count count-elements rle)

  (import (rnrs)
          (dataframe helpers))

  (define (count-elements lst)
    (map (lambda (x) (cons x (count x lst)))
         (remove-duplicates lst)))

  (define (count obj lst)
    (length (filter (lambda (x) (equal? obj x)) lst)))
  
  (define (rle lst)
    ;; run length encoding
    ;; returns a list of pairs where the car and cdr of each pair
    ;; are the values and lengths of the runs, respectively, for the values in lst
    (let loop ([first (car lst)]
               [rest (cdr lst)]
               [n 1]
               [vals '()]
               [counts '()])
      (cond
       [(null? rest)
	(map cons
	     (reverse (cons first vals))
	     (reverse (cons n counts)))]
       [(equal? first (car rest))
	(loop (car rest) (cdr rest) (add1 n) vals counts)]
       [else
	(loop (car rest) (cdr rest) 1 (cons first vals) (cons n counts))])))
  
  )

