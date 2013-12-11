/* scanner for a toy Pascal-like language */

%{
#include "y.tab.h"
extern int yylval;

/* need this for the call to atof() below */
#include <math.h>
%}
%%
DIGIT    [0-9]
ID       [a-z][a-z0-9]*

%%

{DIGIT}+    {
            yyval = atoi( yytext ) );
            return NUM;
            }
{DIGIT}+"."{DIGIT}* {
            yyval = atof(yytext); 
            return NUM;
        }
{ID}    {
            yyval = strdup(yytext);
            return PARAM;
        }

"+" { return PLUS; }
"-" { return MINUS; }
"/" { return DIVIDE; }
"*" { return MULTIPLY; }
"==" { return EQ; }
"!=" { return NE; }
"<" { return GT; }
">" { return LT; }
"<=" { return LE; }
">=" { return GE; }
"(" { return LPAREN; }
") { return RPAREN; }"
"NOT" { return NOT; }
"AND" { return AND; }
"OR" { return OR; }
"XOR" { return XOR; }
"TRUE" { return TRUE; }
"FALSE" { return FALSE; }
}

[ \t\n]+          /* eat up whitespace */

.           printf( "Unrecognized character: %s\n", yytext );

%%
#ifdef yywrap
yywrap() { return 1; }
#endif

int main( int argc, char ** argv) {
    ++argv, --argc;  /* skip over program name */
    if ( argc > 0 )
            yyin = fopen( argv[0], "r" );
    else
            yyin = stdin;

    yylex();
}
