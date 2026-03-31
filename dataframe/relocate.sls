(library (dataframe relocate)
  (export dataframe-relocate)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe assertions)
                check-names))

  (define dataframe-relocate
    (case-lambda
      [(df names)
       (df-relocate df names #f #f)]
      [(df names where anchor)
       (df-relocate df names where anchor)]))

  (define (df-relocate df names where anchor)
    (let ([who "(dataframe-relocate df names)"])
      (check-dataframe df who)
      (check-names names who)
      (apply check-df-names df who names)
      (when anchor
        (unless (symbol? anchor)
          (assertion-violation who "before/after column name must be a symbol"))
        (apply check-df-names df who (list anchor)))
      (when (member anchor names)
        (assertion-violation who "anchor column cannot be one of the relocated columns"))
      (make-dataframe
       (slist-relocate (dataframe-slist df) names where anchor))))

  ;; returns a reordered slist
  (define (slist-relocate slist names where anchor)
    (let* ([all-names (map series-name slist)]
           [other-names (filter (lambda (n) (not (member n names))) all-names)]
           [new-order (build-order other-names names where anchor)])
      (map (lambda (name)
             (car (filter (lambda (s) (symbol=? (series-name s) name)) slist)))
           new-order)))

  ;; build the full ordered list of column names
  (define (build-order other-names names where anchor)
    (cond
      [(not where)
       (append names other-names)]
      [(symbol=? where 'before)
       (insert-before other-names names anchor)]
      [(symbol=? where 'after)
       (insert-after other-names names anchor)]
      [else
       (assertion-violation "(dataframe-relocate)" "where must be 'before or 'after")]))

  ;; insert names immediately before anchor in other-names
  (define (insert-before other-names names anchor)
    (let loop ([rest other-names] [out '()])
      (cond
        [(null? rest)
         (reverse out)]
        [(symbol=? (car rest) anchor)
         (append (reverse out) names (list anchor) (cdr rest))]
        [else
         (loop (cdr rest) (cons (car rest) out))])))

  ;; insert names immediately after anchor in other-names
  (define (insert-after other-names names anchor)
    (let loop ([rest other-names] [out '()])
      (cond
        [(null? rest)
         (reverse out)]
        [(symbol=? (car rest) anchor)
         (append (reverse out) (list anchor) names (cdr rest))]
        [else
         (loop (cdr rest) (cons (car rest) out))])))

  )
