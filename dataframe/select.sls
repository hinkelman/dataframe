(library (dataframe select)
  (export
   dataframe-drop
   dataframe-drop*
   dataframe-select
   dataframe-select*)

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
    (make-dataframe
     (filter (lambda (series)
               (member (series-name series) names))
             (dataframe-slist df))))

  )

