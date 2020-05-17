(library (dataframe aggregate)
  (export dataframe-aggregate
          aggregate-expr)

  (import (chezscheme)
          (dataframe bind)
          (dataframe split)
          (only (dataframe df)
                check-dataframe
                dataframe-values-map
                make-dataframe)   
          (only (dataframe helpers)
                alist-modify
                check-list
                check-names))

  ;; aggregate  ------------------------------------------------------------------------

  ;; aggregate procedures are subtly different from modify procedures
  ;; decided have procedures with redundant functionality rather than working out how to generalize them
  
  ;; same macro as for modify-expr, but named to match the aggregate procedure
  (define-syntax aggregate-expr
    (syntax-rules ()
      [(_ (new-name names expr) ...)
       (list
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...))]))
  
  (define (dataframe-aggregate df group-names aggregate-expr)
    (let ([proc-string "(dataframe-aggregate df group-names aggregate-expr)"])
      (check-dataframe df proc-string)
      (check-list group-names "group-names" proc-string)
      (let-values ([(df-list groups-list) (dataframe-split-helper df group-names #t)])
        (apply dataframe-bind
               (map (lambda (df groups)
                      (df-aggregate-helper df groups aggregate-expr proc-string))
                    df-list groups-list)))))

  (define (df-aggregate-helper df groups aggregate-expr who)
    ;; groups is an alist with one row
    (let* ([new-names (car aggregate-expr)]
           [names-list (cadr aggregate-expr)]
           [proc-list (caddr aggregate-expr)]
           [ls-vals (aggregate-map df names-list proc-list)])
      (check-names new-names who)
      (make-dataframe
       (alist-aggregate-loop groups new-names ls-vals))))

  (define (aggregate-map df names-list proc-list)
    (map (lambda (names proc)
           (list (apply proc (dataframe-values-map df names))))
         names-list proc-list))

  ;; can't just map over columns because won't hold alist structure
  (define (alist-aggregate-loop groups new-names ls-vals)
    (if (null? new-names)
        groups
        (alist-aggregate-loop (alist-modify groups
                                            (car new-names)
                                            (car ls-vals))
                              (cdr new-names)
                              (cdr ls-vals))))
  )

