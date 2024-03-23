;; only bringing procedures here from chez-stats  where want to auto remove 'na (and calculate on booleans)

(library (dataframe statistics)
  (export
   cumulative-sum
   sum
   product
   mean
   weighted-mean
   median
   quantile
   rle)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                add1
                sub1
                make-list
                na?
                remove-na
                remove-duplicates
                transpose))

  (define sum
    (case-lambda
      [(lst) (sum lst #t)]
      [(lst na-rm)
       (let-values ([(total count)
                     (sum/prod/mean lst na-rm + 0 1)])
         total)]))

  (define product
    (case-lambda
      [(lst) (product lst #t)]
      [(lst na-rm)
       (let-values ([(total count)
                     (sum/prod/mean lst na-rm * 1 1)])
         total)]))

  (define mean
    (case-lambda
      [(lst) (mean lst #t)]
      [(lst na-rm)
       (let-values ([(total count)
                     (sum/prod/mean lst na-rm + 0 1)])
         (if (na? total) 'na (/ total count)))]))

  (define weighted-mean
    (case-lambda
      [(lst weights) (weighted-mean lst weights #t)]
      [(lst weights na-rm)
       (let-values ([(total count)
                     (sum/prod/mean lst na-rm + 0 weights)])
         (if (na? total) 'na (/ total count)))]))
  
  (define (sum/prod/mean lst na-rm op init-total weights)
    ;; for everything except weighted mean; weights will be scalar = 1
    (when (list? weights)
      (unless (= (length lst) (length weights))
        (assertion-violation "(weighted-mean lst weights)"
                             "lst and weights are not the same length")))
    (let loop ([lst lst]
               [weights weights]
               [total init-total]
               [count 0])
      (let* ([weight-test (and (list? weights) (not (null? weights)))]
             [weight (if weight-test (car weights) weights)]
             [next-weight (if weight-test (cdr weights) weights)])
        (cond [(null? lst)
               (values total count)]
              ;; any na values in weights yields na
              [(or (na? weight) (and (not na-rm) (na? (car lst))))
               (values 'na 'na)]
              [(na? (car lst))
               (loop (cdr lst) next-weight total count)]
              [(boolean? (car lst))
               (let ([val (if (car lst) 1 0)])
                 (loop (cdr lst) next-weight (op (* val weight) total) (+ weight count)))]
              [else
               (loop (cdr lst) next-weight (op (* (car lst) weight) total) (+ weight count))]))))

  (define (cumulative-sum lst)
    (let ([n (length lst)])
      (let loop ([lst lst]
                 [count 0]
                 [total 0]
                 [out '()])
        (cond [(null? lst)
               (reverse out)]
              [(na? (car lst))
               (append (reverse out) (make-list (- n count) 'na))]
              [(boolean? (car lst))
               (let ([val (if (car lst) 1 0)])
                 (loop (cdr lst) (add1 count) (+ val total) (cons (+ val total) out)))]
               [else
                (loop (cdr lst) (add1 count) (+ (car lst) total)
                      (cons (+ (car lst) total) out))]))))

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

  (define quantile
    ;; type is an integer from 1-9
    ;; 7 is default used for R
    (case-lambda
      [(lst p) (quantile lst p 8 #t)]
      [(lst p type) (quantile lst p type #t)]
      [(lst p type na-rm)
       (let ([lst-sub (remove-na lst)])
         (if (and (not na-rm) (< (length lst-sub) (length lst)))
             'na
             (quantile-helper lst-sub p type)))]))

  (define (quantile-helper lst p type)
    ;; see https://www.jstor.org/stable/2684934 for
    ;; quantile-helper, calc-Q, and get-gamma
    (let* ([n (length lst)]
           [order-stats (list-sort < lst)]
	   ;; ms is list of m values for each quantile type
	   [ms (list 0 0 -1/2 0 1/2 p (- 1 p) (* (add1 p) 1/3) (+ (* p 1/4) 3/8))]
	   [m (list-ref ms (sub1 type))]
	   [j (exact (floor (+ (* n p) m)))]
	   [g (- (+ (* n p) m) j)]
	   [gamma (get-gamma g j type)])
      (calc-Q order-stats j gamma)))

  (define (calc-Q order-stats j gamma)
    ;; j is described for one-based indexing; adjusted for zero-based indexing
    (let ([n-os (length order-stats)])
      (cond
       [(< j 1) (list-ref order-stats 0)]
       [(>= j n-os) (list-ref order-stats (sub1 n-os))]
       [else (+ (* (- 1 gamma) (list-ref order-stats (sub1 j)))
		(* gamma (list-ref order-stats j)))])))
  
  (define (get-gamma g j type)
    (cond
     [(= type 1) (if (= g 0) 0 1)]
     [(= type 2) (if (= g 0) 0.5 1)]
     [(= type 3) (if (and (= g 0) (even? j)) 0 1)]
     [else g]))

  (define median
    (case-lambda
      [(lst) (quantile lst 0.5)]
      [(lst type) (quantile lst 0.5 type)]
      [(lst type na.rm) (quantile lst 0.5 type na.rm)]))

  )

