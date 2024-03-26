(library (dataframe helpers)
  (export
   list-head
   make-list
   add1
   sub1
   iota
   enumerate
   na?
   any-na?
   remove-na
   flatten
   not-in
   remove-duplicates
   rep
   transpose)

  (import (rnrs))

  (define (na? obj)
    (and (symbol? obj)
         (symbol=? obj 'na)))

  (define (any-na? lst)
    (cond [(null? lst) #f]
          [(na? (car lst)) #t]
          [else (any-na? (cdr lst))]))

  (define (remove-na lst)
    (filter (lambda (x) (not (na? x))) lst))

  (define (flatten x)
    (cond ((null? x) '())
          ((not (pair? x)) (list x))
          (else (append (flatten (car x))
                        (flatten (cdr x))))))

  (define (remove-duplicates lst)
    (let ([ht (make-hashtable equal-hash equal?)])
      (let loop ([lst lst]
                 [results '()])
        (cond [(null? lst)
               (reverse results)]
              [(hashtable-ref ht (car lst) #f)
               (loop (cdr lst) results)] ;; value already in ht
              [else
               ;; value is arbitrarily set to 0; key is what matters
               (hashtable-set! ht (car lst) 0) 
               (loop (cdr lst) (cons (car lst) results))]))))

  (define (transpose lst)
    (apply map list lst))

  (define (not-in xs ys)
    (filter (lambda (x) (not (member x ys))) xs))

  (define (rep lst n type)
    (unless (and (symbol? type) (member type '(times each)))
      (assertion-violation
       "(rep lst n type)"
       "type must be 'each or 'times"))
    (if (symbol=? type 'each)
        (apply append (map (lambda (x) (make-list n x)) lst))
        (rep-times lst n)))

  (define (rep-times lst n)
    (define (loop out n)
      (if (= n 1) out (loop (append lst out) (sub1 n))))
    (loop lst n))

  (define (make-list n x)
    (let loop ((n n) (r '()))
      (if (= n 0)
          r
          (loop (- n 1) (cons x r)))))

  (define (sub1 n) (- n 1))
  (define (add1 n) (+ n 1))

  ;; simplified SRFI 1 iota (regular version will work)
  (define (iota count)
    (define start 0)
    (define step 1)
    (let loop ((n 0) (r '()))
      (if (= n count)
	  (reverse r)
	  (loop (+ 1 n)
	        (cons (+ start (* n step)) r)))))

  (define (enumerate lst)
    (iota (length lst)))

  ;; from SRFI-1 `take`
  (define (list-head lst k)
    (let recur ((lst lst) (k k))
      (if (zero? k) '()
	  (cons (car lst)
	        (recur (cdr lst) (- k 1))))))
  )




