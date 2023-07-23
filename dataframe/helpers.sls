(library (dataframe helpers)
  (export
   list-head
   make-list
   add1
   sub1
   iota
   enumerate
   na?
   ;; alist-drop
   ;; alist-ref
   ;; alist-repeat-rows
   ;; alist-values
   ;; alist-values-map
   ;; combine-names-ordered
   ;; filter-ls-vals
   ;; filter-vals
   flatten
   ;; get-all-names
   ;; get-all-unique-names
   ;; cartesian-product
   not-in
   ;; partition-ls-vals
   ;; rep
   remove-duplicates
   transpose)

  (import (rnrs))

  (define (na? obj)
    (and (symbol? obj)
         (symbol=? obj 'na)))

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
    (cond [(symbol=? type 'each)
           (apply append (map (lambda (x) (make-list n x)) lst))]
          [(symbol=? type 'times)
           (rep-times lst n)]
          [else
           (assertion-violation "(rep ls n type)"
                                "type must be 'each or 'times")]))

  (define (rep-times lst n)
    (define (loop out n)
      (if (= n 1) out (loop (append lst out) (sub1 n))))
    (loop lst n))



  ;; ls-vals ------------------------------------------------------------------------
  
  ;; in previous version, would pass over ls-vals twice with filter-ls-vals (with bools negated on 1 pass)
  ;; the extra transposing in this version is faster than two passes with filter-ls-vals
  (define (partition-ls-vals bools ls-vals)
    (let loop ([bools bools]
               [ls-rows (transpose ls-vals)]
               [keep '()]
               [drop '()])
      (if (null? bools)
          (values (transpose (reverse keep)) (transpose (reverse drop)))
          (if (car bools)
              (loop (cdr bools) (cdr ls-rows) (cons (car ls-rows) keep) drop)
              (loop (cdr bools) (cdr ls-rows) keep (cons (car ls-rows) drop))))))
  
  (define (filter-ls-vals bools ls-vals)
    (map (lambda (vals) (filter-vals bools vals)) ls-vals))

  ;; filter vals by list of booleans of same length as vals
  (define (filter-vals bools vals)
    (let ([bools-vals (map cons bools vals)])
      (map cdr (filter (lambda (x) (car x)) bools-vals))))

  ;; alists ------------------------------------------------------------------------

  (define (alist-values alist name)
    (cdr (assoc name alist)))

  (define (alist-values-map alist names)
    (map (lambda (name) (alist-values alist name)) names))

  ;; expand an list by repeating rows n times (or each)
  (define (alist-repeat-rows alist n type)
    (map (lambda (ls) (cons (car ls) (rep (cdr ls) n type))) alist))

  (define (get-all-names . alists)
    (apply append (map (lambda (alist) (map car alist)) alists)))

  (define (get-all-unique-names . alists)
    (remove-duplicates (apply get-all-names alists)))

  ;; combine names such that they stay in the order that they appear in each dataframe
  (define (combine-names-ordered . alists)
    (define (loop all-names results)
      (cond [(null? all-names)
             (reverse results)]
            [(member (car all-names) results)
             (loop (cdr all-names) results)]
            [else
             (loop (cdr all-names) (cons (car all-names) results))]))
    (loop (apply get-all-names alists) '()))
  
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




