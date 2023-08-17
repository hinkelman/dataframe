(library (dataframe statistics)
  (export
   add
   multiply
   cumulative-sum
   sum
   mean
   median
   list-min
   list-max
   quantile
   rle)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                add1
                sub1
                make-list
                na?
                remove-duplicates
                transpose))

  (define (add . lst)
    (add/multiply lst +))
    
  (define (multiply . lst)
    (add/multiply lst *))
 
  (define (add/multiply lst proc)
    (map (lambda (row)
           (apply proc (filter (lambda (x) (not (na? x))) row)))
         (transpose lst)))
    
  (define sum
    (case-lambda
      [(lst) (sum lst #t)]
      [(lst na-rm) (sum/mean lst na-rm 'sum)]))

  (define mean
    (case-lambda
      [(lst) (mean lst #t)]
      [(lst na-rm) (sum/mean lst na-rm 'mean)]))
  
  (define (sum/mean lst na-rm type)
    (let loop ([lst lst]
               [total 0]
               [count 0])
      (cond [(null? lst)
             (if (symbol=? type 'sum) total (/ total count))]
            [(and (na? (car lst)) (not na-rm))
             'na]
            [(na? (car lst))
             (loop (cdr lst) total count)]
            [(and (boolean? (car lst)) (not (car lst)))
             (loop (cdr lst) total (add1 count))]
            [(and (boolean? (car lst)) (car lst))
             (loop (cdr lst) (add1 total) (add1 count))]
            [else
             (loop (cdr lst) (+ (car lst) total) (add1 count))])))

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
              [(and (boolean? (car lst)) (not (car lst)))
               (loop (cdr lst) (add1 count) total (cons total out))]
              [(and (boolean? (car lst)) (car lst))
               (loop (cdr lst) (add1 count) (add1 total) (cons (add1 total) out))]
              [else
               (loop (cdr lst) (add1 count) (+ (car lst) total)
                     (cons (+ (car lst) total) out))]))))

  ;; need different names for min/max to avoid name collision
  (define list-min
    (case-lambda
      [(lst) (list-min lst #t)]
      [(lst na-rm) (min/max lst na-rm 'min)]))

  (define list-max
    (case-lambda
      [(lst) (list-max lst #t)]
      [(lst na-rm) (min/max lst na-rm 'max)]))

  (define (min/max lst na-rm type)
    ;; not including boolean here b/c seems less useful
    (let ([comp (if (symbol=? type 'min) < >)])
      (let loop ([lst lst]
                 [result (if (symbol=? type 'min) +inf.0 -inf.0)])
        (cond [(null? lst) result]
              [(and (na? (car lst)) (not na-rm)) 'na]
              [(na? (car lst))
               (loop (cdr lst) result)]
              [else
               (loop (cdr lst) (if (comp (car lst) result)
                                   (car lst)
                                   result))]))))
  
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
       (let ([lst-sub (filter (lambda (x) (not (na? x))) lst)])
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

