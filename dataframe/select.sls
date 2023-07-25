(library (dataframe select)
  (export
   dataframe-drop
   dataframe-drop*
   dataframe-select
   dataframe-select*
   dataframe-series
   dataframe-values
   slist-drop
   slist-select
   $)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe helpers)
                not-in))

  ;; lots of procedures here for a simple operation
  ;; want to provide macros for easier interactive use
  ;; and non-macro versions for programmatic use
  
  (define-syntax dataframe-select* 
    (syntax-rules ()
      [(_ df name ...)
       (dataframe-select df (list (quote name) ...))]))

  (define-syntax dataframe-drop* 
    (syntax-rules ()
      [(_ df name ...)
       (dataframe-drop df (list (quote name) ...))]))
  
  (define (dataframe-select df names)
    (apply check-df-names df "(dataframe-select df names)" names)
    (df-select/drop df names 'select))

  (define (dataframe-drop df names)
    (apply check-df-names df "(dataframe-drop df names)" names)
    (df-select/drop df names 'drop))

  (define (df-select/drop df names type)
    (let ([select-names (if (symbol=? type 'select)
                            names
                            (not-in (dataframe-names df) names))])
      (df-select df select-names)))

  (define (df-select df names)
    (make-dataframe (slist-select (dataframe-slist df) names)))

  (define (slist-select slist names)
    (map (lambda (name)
           ;; output of filter should be list of length one
           (car (filter (lambda (series) (symbol=? (series-name series) name)) slist)))
         names))

  (define (slist-drop slist names)
    (map (lambda (name)
           ;; output of filter should be list of length one
           (car (filter (lambda (series) (not (symbol=? (series-name series) name))) slist)))
         names))
  
  ;; extract values -----------------------------------------------------------------

  (define (dataframe-series df name)
    (let ([who "(dataframe-series df name)"])
      (check-dataframe df who)
      (check-names-exist df who name))
    (car (dataframe-slist (df-select df (list name)))))
  
  ;; returns simple list
  (define (dataframe-values df name)
    (let ([who "(dataframe-values df name)"])
      (check-dataframe df who)
      (check-names-exist df who name))
    (series-lst (dataframe-series df name)))

  (define ($ df name)
    (dataframe-values df name))

  )

