
(library (dataframe display)
  (export dataframe-display)

  (import (rnrs)
          (slib format)
          (only (dataframe df)
                check-dataframe
                dataframe?
                dataframe-alist
                dataframe-dim
                dataframe-head
                dataframe-names
                dataframe-values-map)    
          (only (dataframe helpers)
                add1
                sub1
                list-head
                transpose))

   (define dataframe-display
    (case-lambda
      [(df) (df-display-helper df 10 5 80)]
      [(df n) (df-display-helper df n 5 80)]
      [(df n min-width) (df-display-helper df n min-width 80)]
      [(df n min-width total-width) (df-display-helper df n min-width total-width)]))

  (define (df-display-helper df n min-width total-width)
    (let ([who "(dataframe-display df)"])
      (check-dataframe df who)
      (unless (> total-width min-width)
        (assertion-violation who "total-width must be greater than min-width")))
    (let* ([dim (dataframe-dim df)]
           [rows (car dim)]
           [n-actual (if (< rows n) rows n)])
      (format-df (dataframe-head df n-actual) dim min-width total-width)))

  (define (format-df df dim min-width total-width)
    (let* ([names (dataframe-names df)]
           [ls-vals (dataframe-values-map df names)]
           [prep-vals (map prepare-non-numbers ls-vals)]
           [parts (build-format-parts names prep-vals min-width total-width 2)])
      (format #t " dim: ~d rows x ~d cols" (car dim) (cdr dim))
      (format #t (cdr (assoc 'header parts)) (cadr (assoc 'rowtable parts)))
      (format #t (cdr (assoc 'table parts)) (cddr (assoc 'rowtable parts)))
      (newline)
      (display (cdr (assoc 'footer parts)))))

  ;; returns numeric list unchanged
  ;; elements of all other lists can be boolean, character, string, or symbol
  ;; numbers in mixed-type lists are converted to strings
  (define (prepare-non-numbers lst)
    (let ([types (map get-type lst)])
      (if (for-all number? lst)
          lst
          (map (lambda (obj typ) (get-display-value obj typ)) lst types))))
  
  ;; `null?` needs to be before `list?` b/c null is also a list 
  (define (get-type object)
    (let loop ([preds (list boolean? char? dataframe? hashtable?
                            null? list? number? pair? string? symbol? vector?)]
               [types '(boolean char dataframe hashtable
                                null list number pair string symbol vector)])
      (if (null? preds)
          "other"
          (if ((car preds) object)
              (car types)
              (loop (cdr preds) (cdr types)))))) 

  (define (get-display-value object type)
    (cond [(member type '(boolean char number string symbol)) object]
          [(symbol=? type 'other) "<other type>"]
          [else (string-append "<" (symbol->string type) ">")]))

  (define (build-format-parts names prep-vals min-width total-width pad)
    (let* ([e-dec 3]
           [format-parts (map (lambda (lst)
                                (compute-format-parts lst e-dec pad)) prep-vals)]
           [val-widths (map-efp format-parts 'width)]
           [col-widths (compute-column-widths names val-widths min-width pad)]
           [num-types (map-efp format-parts 'num-type)]
           [declst (map-efp format-parts 'decimal)]
           [esiglst (map-efp format-parts 'esigfig)])
      (let loop ([names names]
                 [prep-vals prep-vals]
                 [nt num-types]
                 [cw col-widths]
                 [dec declst]
                 [esig esiglst]
                 [used-width 0]
                 [used-names '()]
                 [used-vals '()]
                 [hd "~& ~{"]
                 [tbl "~:{~& "])
        (if (or (null? names) (>= (+ used-width (car cw)) total-width))
            (list (cons 'header (string-append hd "~}"))
                  (cons 'table (string-append tbl "~}"))
                  (cons 'rowtable (transpose (map (lambda (name vals)
                                                    (cons name vals))
                                                  (reverse used-names) (reverse used-vals))))
                  (cons 'footer (format-footer names)))
            (let* ([width-part (string-append "~" (number->string (car cw)))]
                   [hdr-part (string-append width-part "@a ")]
                   [full-part (if (string=? (car nt) "nan")
                                  hdr-part ;; same as tbl when not numbers
                                  (string-append
                                   width-part
                                   (format-number (car nt) (car dec) (car esig))))])
              (loop (cdr names)
                    (cdr prep-vals)
                    (cdr nt)
                    (cdr cw)
                    (cdr dec)
                    (cdr esig)
                    (+ used-width (car cw))
                    (cons (car names) used-names)
                    (cons (car prep-vals) used-vals)
                    (string-append hd hdr-part)
                    (string-append tbl full-part)))))))

  (define (compute-format-parts lst e-dec pad)
    (if (not (for-all number? lst))
        (let ([widthlst (map (lambda (x) (compute-object-width x pad)) lst)])
          (list (cons 'width (apply max widthlst))))
        (let* ([neg (if (any-negative? lst) 1 0)]
               [lst (map abs lst)]
               [siglst (map compute-sigfig lst)]
               [elst (map compute-expt lst)]
               [esig (apply max (map compute-sigfig elst))]
               [declst (map (lambda (x sig e)
                              (compute-decimal x sig e e-dec))
                            lst siglst elst)]
               [num-type (if (> (length (filter (lambda (x) (= x e-dec)) declst)) 0) "e" "f")]
               [widthlst (map (lambda (sig dec)
                                (compute-num-width num-type neg sig esig dec pad))
                              siglst declst)]
               [dec (if (string=? num-type "e") e-dec (apply max declst))])
          (list (cons 'num-type num-type)
                (cons 'width (apply max widthlst))
                (cons 'decimal dec)
                (cons 'esigfig esig)))))
  
  (define (compute-num-width num-type neg sig esig dec pad)
    (if (string=? num-type "e")
        (+ neg esig dec pad 3)
        (+ neg sig dec pad)))

  ;; numbers in mixed-type columns are displayed differently than numbers in number-only columns
  ;; for reasons that I don't currently understand, this math leads to extra space in format (hence sub1)
  (define (compute-object-width object pad)
    (let ([obj-width
           (cond [(boolean? object)
                  (string-length (if object "#t" "#f"))]
                 [(char? object)
                  (string-length (string object))]
                 [(number? object)
                  (string-length (number->string object))]
                 [(string? object)
                  (string-length object)]
                 [(symbol? object)
                  (string-length (symbol->string object))]
                 [else
                  (assertion-violation
                   "(compute-width object pad)"
                   "object of this type was not anticipated")])])
      (sub1 (+ pad obj-width))))

  (define (map-efp format-parts part)
    (map (lambda (lst) (extract-format-parts lst part)) format-parts))
  
  (define (extract-format-parts lst part)
    (if (and (not (symbol=? part 'width))
             (not (assoc 'num-type lst)))
        "nan"
        (cdr (assoc part lst))))

  (define (compute-column-widths names val-widths min-width pad)
    (let ([name-widths (map (lambda (name) (compute-object-width name pad)) names)])
      (map (lambda (name-width val-width)
             (max min-width name-width val-width)) name-widths val-widths)))

  (define (any-negative? lst)
    (> (length (filter negative? lst)) 0))

  (define (format-number num-type dec esig)
    (let ([dec-part (string-append "," (number->string dec))])
      (if (string=? num-type "f")
          (string-append dec-part "f ")
          (string-append dec-part "," (number->string esig) "e "))))
  
  ;; returns number of decimal digits to use
  ;; return value of 3 indicates that number should be displayed in scientific notation
  (define (compute-decimal x sigfig e e-dec)
    (let ([default 4]
          [x (abs x)])
      (cond [(or (< e -3) (> e 5)) e-dec] ;; easier to read big numbers than small numbers
            [(integer? x) 0]
            [(> e 3) 2]                   ;; fewer decimals for larger numbers 
            [(and (< x 1) (> sigfig default)) sigfig]
            [else default])))

  ;; https://github.com/r-lib/pillar/blob/master/R/sigfig.R
  (define (compute-expt x)
    (let* ([x (abs x)]
           [digits 4]
           [offset (/ (log (add1 (* -5 (expt 10 (sub1 (* -1 digits)))))) (log 10))])
      (if (zero? x)
          0
          (exact
           (floor (- (log10 x) offset))))))

  ;; noticed that (log 1000 10) returned 2.999999...
  (define (log10 x)
    (let* ([eps 1e-10]
           [logx (log x 10)]
           [rlx (round logx)])
      (if (and (> rlx (- logx eps))
               (< rlx (+ logx eps)))
          rlx
          logx)))

  (define (compute-sigfig x)
    (let ([x (abs x)])
      (if (zero? x)
          1
          (exact
           (add1 (floor (log10 x)))))))

  ;; used for displaying column names that don't fit in the specified width
  (define (format-footer names)
    (if (null? names)
        ""
        (string-append " Columns not displayed: " (add-commas names) "\n")))
  
  ;; add commas to list of strings that are being appended
  (define (add-commas ls)
    (let ([ls (map symbol->string ls)])
      (apply string-append
             (let loop ([ls (cdr ls)]
                        [results (list (car ls))])
               (if (null? ls)
                   (reverse results)
                   (loop (cdr ls) (cons (car ls) (cons ", " results))))))))

  )



