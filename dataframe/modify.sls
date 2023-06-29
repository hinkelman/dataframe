(library (dataframe modify)
  (export dataframe-modify
          dataframe-modify*
          dataframe-modify-all
          dataframe-modify-at)

  (import (rnrs)
          (only (dataframe df)
                check-dataframe
                check-df-names
                dataframe-alist
                dataframe-names
                make-dataframe)   
          (only (dataframe helpers)
                make-list
                alist-values-map
                check-names))

  ;; modify/add columns ------------------------------------------------------------------------

  (define-syntax dataframe-modify*
    (syntax-rules ()
      [(_ df (new-name names expr) ...)
       (df-modify
        df
        (list (quote new-name) ...)
        (list (quote names) ...)
        (list (lambda names expr) ...)
        "(dataframe-modify* df (new-name names expr) ...)")]))

  (define (dataframe-modify df new-names names . procedure)
    (df-modify df new-names names procedure
               "(dataframe-modify df new-names names procedure ...)"))

  (define (df-modify df new-names names procs who)
    ;; can't check new-names here because they might be duplicated b/c
    ;; different expr acting on the same column
    (check-dataframe df who)
    (unless (= (length new-names) (length names) (length procs))
      (assertion-violation
       who
       "new-names, names, and procedures lists must be the same length"))
    (let ([alist (dataframe-alist df)])
      (make-dataframe
       (alist-modify-loop alist new-names names procs who))))

  ;; can't just map over columns because won't hold alist structure
  ;; also don't want to map over procedures
  ;; because want to update alist after each procedure
  (define (alist-modify-loop alist new-names names procedures who)
    (if (null? new-names)
        alist
        (alist-modify-loop (alist-modify alist
                                         (car new-names)
                                         (modify-map alist
                                                     (car names)
                                                     (car procedures)
                                                     who))
                           (cdr new-names)
                           (cdr names)
                           (cdr procedures)
                           who)))

  ;; update alist and or add column to end of alist
  ;; based on whether name is already present in alist
  (define (alist-modify alist name vals)
    (let ([col (cons name vals)]
          [all-names (map car alist)])
      (if (member name all-names)
          (map (lambda (x)
                 (if (symbol=? x name) col (assoc x alist)))
               all-names)
          ;; to get correct structure with append,
          ;; need to make col an alist (by wrapping in a list) 
          (append alist (list col))))) 
  
  (define (modify-map alist names proc who)
    (if (null? names)
        (modify-map-helper alist (proc) who)
        (apply map proc (alist-values-map alist names))))

  ;; this helper procedure returns vals for a column from
  ;; a scalar or list of same length as number of df rows
  (define (modify-map-helper alist vals who)
    (let ([alist-rows (length (cdar alist))])
      (cond [(scalar? vals)
             (make-list alist-rows vals)]
            [(and (list? vals) (= (length vals) alist-rows))
             vals]
            [else
             (assertion-violation
              who
              (string-append
               "value(s) must be scalar or list of length "
               (number->string alist-rows)))])))

  (define (scalar? obj)
    (or (symbol? obj) (char? obj) (string? obj) (number? obj)))

  (define (dataframe-modify-all df procedure)
    (df-modify-at-all-helper df
                             procedure
                             (dataframe-names df)
                             "(dataframe-modify-at df procedure)"))
  
  (define (dataframe-modify-at df procedure . names)
    (df-modify-at-all-helper df
                             procedure
                             names
                             "(dataframe-modify-at df procedure names)"))

  (define (df-modify-at-all-helper df procedure names who)
    (apply check-df-names df who names)
    (make-dataframe (alist-modify-at (dataframe-alist df)
                                     procedure
                                     names)))

  ;; could just map over alist-modify but this way avoids name checking
  ;; because columns are modified with same name, not appending new column
  (define (alist-modify-at alist procedure names)
    (map (lambda (column)
           (if (member (car column) names)
               (cons (car column) (map procedure (cdr column)))
               column))
         alist))

  ;; (dataframe-list-modify) doesn't work when making a "non-vectorized" calculation,
  ;; e.g., (mean ($ df 'count), and, thus, doesn't seem that useful 
  ;; (define (dataframe-list-modify df-list modify-expr)
  ;;   (apply dataframe-bind (map (lambda (df) (dataframe-modify df modify-expr)) df-list)))
  
  )
