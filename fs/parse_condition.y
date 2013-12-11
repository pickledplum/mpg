% { 
// c declarations 
}

// Bison declarations
%start ROOT

%token PLUS
%token MINUS
%token DIVIDE
%token MULTIPLY
%token EQ
%token NE
%token GT
%token LT
%token LE
%token GE
%token LPAREN
%token RPAREN
%token NOT
%token AND
%token OR
%token XOR
%token TRUE
%token FALSE
%token UPLUS
%token UMINUS
%union {
    double real;
    symrec *tptr;
}
%token <real> NUM

%left LE RE LT RT EQ NE
%left PLUS MINUS
%left MULTIPLY DIVIDE
%left UMINUS UPLUS

// Grammer rules
%%

ROOT :
    logical_expr { printf("Done!\n");  }
    ;
quant_expr : 
    quant_primary { $$ = $1; }
    | quant_expr MINUS quant_expr { $$ = $1 - $3 }
    | quant_expr PLUS quant_expr { $$ = $1 + $3 }
    | quant_expr MULTIPLY quant_expr { $$ = $1 * $3 }
    | quant_expr DIVIDE quant_expr { $$ = $1 / $3 }
    | MINUS quant_expr %prec UMINUS
    | PLUS quant_expr %prec UPLUS
    ;
logical_expr : /* empty */ { $$ = 1; }
    | logical_primary { $$ = $1; }
    | quant_expr LT quant_expr { $$ = $1 < $3; }
    | quant_expr GT quant_expr { $$ = $1 > $3; }
    | quant_expr LE quant_expr { $$ = $1 <= $3; }
    | quant_expr GE quant_expr { $$ = $1 >= $3; }
    | quant_expr EQ quant_expr { $$ = $1 == $3; } 
    | quant_expr EQ quant_expr { $$ = $1 != $3; } 
    | logical_expr AND logical_expr { $$ = $1 && $3; }
    | logical_expr OR logical_expr { $$ = $1 || $3; }
    | logical_expr XOR logical_expr { $$ = $1 xor $3; }
    | NOT logical_expr { $$ = !$2; }
    ;
logical_primary : 
    TRUE { $$ = 1; }
    | FALSE { $$ = 0; }
    | LPARAN logical_expr RPARAN { $$ = $2; }
    ;
quant_primary : 
    PARA { $$ = $1; }
    | NUM { $$ = $1; }
    | LPARAN quant_expr RPARAN { $$ = $2; }
    ;

%%

// Additional code