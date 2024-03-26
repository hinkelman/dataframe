;; only bringing procedures here from chez-stats where want to autoremove 'na (and calculate on booleans)
;; rank has more complicated 'na handling (in R) so not bringing over from chez-stats now

(library (dataframe statistics)
  (export
   cumulative-sum
   sum
   product
   mean
   weighted-mean
   variance
   standard-deviation
   median
   quantile
   interquartile-range
   rle)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                add1
                sub1
                make-list
                na?
                any-na?
                remove-na))

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

  (define variance
    (case-lambda
      [(lst) (variance-helper lst #t)]
      [(lst na-rm) (variance-helper lst na-rm)]))

  (define (variance-helper lst na-rm)
    ;; https://www.johndcook.com/blog/standard_deviation/
    (define (update-ms x ms i)
      ;; ms is a pair of m and s variables
      ;; x is current value of lst in loop
      (let* ([m (car ms)]
	     [s (cdr ms)]
	     [new-m (+ m (/ (- x m) (add1 i)))]
             [new-s (+ s (* (- x m) (- x new-m)))])
	(cons new-m new-s)))
    (let loop ([lst (cdr lst)]
	       [ms (cons (car lst) 0)] 
	       [i 1])                 ; one-based indexing in the algorithm
      (cond [(null? lst)
	     (/ (cdr ms) (- i 1))]
            [(and (not na-rm) (na? (car lst)))
             'na]
            [(na? (car lst))
             (loop (cdr lst) ms i)]
            [else
             (loop (cdr lst) (update-ms (car lst) ms i) (add1 i))])))
  
  (define standard-deviation
    (case-lambda
      [(lst) (standard-deviation lst #t)]
      [(lst na-rm)
       (let ([var (variance lst na-rm)])
         (if (na? var) 'na (sqrt var)))]))

  (define quantile
    ;; type is an integer from 1-9
    ;; 7 is default used for R
    (case-lambda
      [(lst p) (quantile lst p 8 #t)]
      [(lst p type) (quantile lst p type #t)]
      [(lst p type na-rm)
       (if (and (not na-rm) (any-na? lst))
           'na
           ;; on large lists, this remove na step is costly (considerably more so than any-na above)
           ;; but the iteration happens in list-sort in quantile-helper
           ;; so would have to write custom sort that bows out when encountering the first na
           ;; maybe do that later???
           (quantile-helper (remove-na lst) p type))]))

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
      [(lst) (quantile lst 0.5 8 #t)]
      [(lst type) (quantile lst 0.5 type #t)]
      [(lst type na-rm) (quantile lst 0.5 type na-rm)]))

  (define interquartile-range
    (case-lambda
      [(lst) (interquartile-range lst 8 #t)]
      [(lst type) (interquartile-range lst type #t)]
      [(lst type na-rm)
       (let ([lwr (quantile lst 0.25 type na-rm)]
             [upr (quantile lst 0.75 type na-rm)])
         (if (or (na? lwr) (na? upr)) 'na (- upr lwr)))]))

  )

