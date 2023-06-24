(library (dataframe aggregate)
  (export dataframe-aggregate
          dataframe-aggregate*)

  (import (rnrs)
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

  (define-syntax dataframe-aggregate*
    (syntax-rules ()
      [(_ df group-names ((new-name names expr) ...))
       (df-aggregate
        df
        (quote group-names)
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...)
        "(dataframe-aggregate* df group-names (new-name names expr))")]))
  
  (define (dataframe-aggregate df group-names new-names names procs)
    (df-aggregate df group-names new-names names procs
                  "(dataframe-aggregate df group-names new-names procs)"))

  (define (df-aggregate df group-names new-names names procs who)
    (check-dataframe df who)
    (check-names new-names who)
    (check-list group-names "group-names" who)
    (let-values ([(df-list groups-list) (dataframe-split-helper df group-names #t)])
      (apply dataframe-bind
             (map (lambda (df groups)
                    (df-aggregate-helper df groups new-names names procs))
                  df-list groups-list))))
    
  (define (df-aggregate-helper df groups new-names names procs)
    ;; groups is an alist with one row
    (let ([ls-vals (aggregate-map df names procs)])
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

