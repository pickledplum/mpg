%{
	#include <cstdio>
	#include <map>
	#include "node.h"
	extern int yylex();
	void yyerror(const char*s) { printf("ERROR: %s\n", s); }
	SymbolTable *g_symbol_table;
	Node *g_expr;
%}

%union{
	Node *node;
	std::string *string;
	char character;
	int token;
	float numeric;
}

%token <string> FSPARAM INTEGER DOUBLE
%token <token> LPAREN RPAREN COMMA
%token <character> PLUS MINUS MUL DIV 
%token <string> ABS MIN MAX

%type <node> expr param number terminal
%type <string> binary_func unary_func

%left PLUS MINUS
%left MUL DIV
%left NEG

%start input

%%
input : { g_symbol_table = new SymbolTable(); } expr
	{ 
		g_expr = $2;
		printf("\nBefore substitution... %f\n", $2->eval()); 
		printf("Leaving yyparse()\n");
		YYACCEPT;
	}
	;

param : FSPARAM { FSParam* fsparam = new FSParam(*$1);  (*g_symbol_table)[*$1] = fsparam;  $$ = fsparam; } 
	;
number : INTEGER { $$ = new Numeric(atoi($1->c_str())); delete $1; }
	| DOUBLE { $$ = new Numeric(atof($1->c_str())); delete $1; }
	;
terminal : param | number
	;
expr :  terminal
	| expr DIV expr { $$ = new BinaryOperator($1,$2,$3); }
	| expr MUL expr { $$ = new BinaryOperator($1,$2,$3); }
	| expr PLUS expr { $$ = new BinaryOperator($1,$2,$3); }
	| expr MINUS expr { $$ = new BinaryOperator($1,$2,$3); }
	| MINUS expr %prec NEG { $$ = new BinaryOperator( new Numeric(-1), '*', $2); }
	| LPAREN expr RPAREN { $$ = $2; }
	| unary_func LPAREN expr RPAREN { $$ = new UnaryFunction(*$1, $3); }
	| binary_func LPAREN expr COMMA expr RPAREN { $$ = new BinaryFunction(*$1,$3,$5); }
	;

unary_func :
	ABS 
	;
binary_func : 
	MIN | MAX
	;
%%
