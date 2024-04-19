(library (dataframe modify)
  (export dataframe-modify
          dataframe-modify*
          dataframe-modify-all
          dataframe-modify-at)

  (import (rnrs)
          (dataframe record-types)
          (only (dataframe select)
                slist-select)
          (only (dataframe helpers)
                make-list
                na?))

  ;; modify/add columns ------------------------------------------------------------------------

  (define-syntax dataframe-modify*
    (syntax-rules ()
      [(_ df (new-name names expr) ...)
       (df-modify-loop
        df
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...)
        "(dataframe-modify* df (new-name names expr) ...)")]))

  (define (dataframe-modify df new-names names . procedures)
    (let ([who "(dataframe-modify df new-names names procedure ...)"])
      ;; can't check new-names here; they might be duplicated b/c
      ;; different expr acting on the same column
      (check-dataframe df who)
      (unless (= (length new-names) (length names) (length procedures))
        (assertion-violation
         who
         "new-names, names, and procedures lists must be the same length"))
      (df-modify-loop df new-names names procedures who)))

  ;; don't want to map over procedures
  ;; because want to update df after each procedure
  (define (df-modify-loop df new-names names procedures who)
    (if (null? new-names)
        df
        (df-modify-loop
         (df-modify df (map-proc df (car new-names) (car names) (car procedures) who))
         (cdr new-names)
         (cdr names)
         (cdr procedures)
         who)))

  ;; update df and/or add column to end of df
  ;; based on whether name is already present in df
  (define (df-modify df new-series)
    (let* ([slist (dataframe-slist df)]
           [new-name (series-name new-series)]
           [all-names (map series-name slist)])
      (make-dataframe
       (if (member new-name all-names)
           (map (lambda (series)
                  (if (symbol=? (series-name series) new-name) new-series series))
                slist)
           ;; need to wrap in list for appending
           (append slist (list new-series)))))) 
  
  (define (map-proc df new-name names proc who)
    (make-series
     new-name
     (if (null? names)
         (map-proc-helper (car (dataframe-dim df)) (proc) who)
         (let ([slist-sel (slist-select (dataframe-slist df) names)])
           (apply map proc (map series-lst slist-sel))))))

  ;; this helper procedure returns vals for a column from
  ;; a scalar or list of same length as number of df rows
  (define (map-proc-helper df-rows vals who)
    (cond [(scalar? vals)
           (make-list df-rows vals)]
          [(and (list? vals) (= (length vals) df-rows))
           vals]
          [else
           (assertion-violation
            who
            (string-append
             "value(s) must be scalar or list of length "
             (number->string df-rows)))]))

  (define (scalar? obj)
    (or (na? obj) (boolean? obj) (symbol? obj)
        (char? obj) (string? obj) (number? obj)))

  (define (dataframe-modify-all df procedure)
    (check-dataframe df "(dataframe-modify-all df procedure)")
    (df-modify-at df procedure (dataframe-names df)))

  (define (dataframe-modify-at df procedure . names)
    (apply check-df-names df "(dataframe-modify-at df procedure names)" names)
    (df-modify-at df procedure names))

  ;; because columns are modified with same name, not appending new column
  (define (df-modify-at df procedure names)
    (make-dataframe
     (map (lambda (series)
            (if (member (series-name series) names)
                (make-series (series-name series) (map procedure (series-lst series)))
                series))
          (dataframe-slist df))))

  )
