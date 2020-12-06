
(library (dataframe display)
  (export dataframe-display)

  (import (chezscheme)
          (only (dataframe df)
                check-dataframe
                dataframe-alist)    
          (only (dataframe helpers)
                transpose))

  ;; lack of familiarity with `format` meant reinventing the wheel
  ;; maybe could rewrite with format directives to make more concise and less brittle
  ;; today is not the day for that rewrite, though
  (define dataframe-display
    (case-lambda
      [(df) (df-display-helper df 10 2 10 80)]
      [(df n) (df-display-helper df n 2 10 80)]
      [(df n pad) (df-display-helper df n pad 10 80)]
      [(df n pad min-width) (df-display-helper df n pad min-width 80)]
      [(df n pad min-width total-width) (df-display-helper df n pad min-width total-width)]))

  (define (df-display-helper df n pad min-width total-width)
    (let ([proc-string "(dataframe-display df n pad min-width total-width)"])
      (check-dataframe df proc-string)
      (unless (> min-width pad)
        (assertion-violation proc-string "min-width must be greater than pad"))
      (unless (> total-width min-width)
        (assertion-violation proc-string "total-width must be greater than min-width")))
    (alist-display (dataframe-alist df) n pad min-width total-width))

  (define (alist-display alist n pad min-width total-width)
    (let* ([names (map (lambda (col)
                         (symbol->string (car col))) alist)]
           [ls-vals (map cdr alist)]
           [rows (length (cdar alist))]
           [n-actual (if (< rows n) rows n)]
           [ls-vals-n (map (lambda (vals) (list-head vals n-actual)) ls-vals)]
           [ls-vals-n-string (map (lambda (ls) (map object->string ls)) ls-vals-n)]
           [widths (column-widths names ls-vals-n ls-vals-n-string)]
           [row-strings (prepare-columns names ls-vals-n-string widths pad min-width total-width)])
      (for-each (lambda (row) (display row) (newline)) row-strings)))

  (define (object->string object)
    (cond [(number? object) (number->string object)]
          [(symbol? object) (symbol->string object)]
          [(boolean? object) (if object "#t" "#f")]
          [(string? object)  object]
          [else "compound data"]))
  
  ;; if a column is numeric, then potentially truncated to length of column name 
  (define (column-widths names ls-vals ls-vals-string)
    (map (lambda (name vals vals-string)
           (if (for-all number? vals)
               (string-length name)
               (apply max (map string-length (cons name vals-string)))))
         names ls-vals ls-vals-string))

  (define (prepare-columns names ls-vals-string widths pad min-width total-width)
    (define (loop names ls-vals-string widths used-width results)
      (if (or (null? names) (> used-width total-width))
          (prepare-results names results)
          (let* ([pad+width (+ pad (car widths))]
                 [column-width (if (> min-width pad+width) min-width pad+width)])
            (loop (cdr names)
                  (cdr ls-vals-string)
                  (cdr widths)
                  (+ used-width column-width)
                  (cons (map (lambda (string)
                               (string-adjust-length string column-width pad))
                             (cons (car names) (car ls-vals-string)))
                        results)))))
    (loop names ls-vals-string widths 0 '()))

  ;; pad or trunctate string
  (define (string-adjust-length string column-width pad)
    (let* ([max-width (- column-width pad)]
           [string-new (if (> (string-length string) max-width)
                           (string-truncate! string max-width)
                           string)])
      (string-append
       (make-string (- column-width (string-length string-new)) #\ )
       string-new)))

  (define (prepare-results names results)
    (let ([last-row (if (null? names)
                        ""
                        (apply string-append (cons "Columns not displayed: " (add-commas names))))]
          [results-new (map (lambda (row) (apply string-append row))
                            (transpose (reverse results)))])
      (reverse (cons last-row (reverse results-new)))))

  ;; add commas to list of strings that are being appended
  (define (add-commas ls)
    (define (loop ls results)
      (if (null? ls)
          (reverse results)
          (loop (cdr ls) (cons (car ls) (cons ", " results)))))
    (loop (cdr ls) (list (car ls))))


  )

