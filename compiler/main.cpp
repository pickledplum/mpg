#include <cstdio>
#include <cstdlib>
#include "node.h"
extern FILE* yyin;
extern int yyparse();
extern SymbolTable *g_symbol_table;
extern Node * g_expr;

int main(int argc, char **argv)
{
	yyin = fopen("junk.in", "r");
    yyparse();
	(*g_symbol_table)["P1"]->setValue(4);
	(*g_symbol_table)["P2"]->setValue(60);

	print_table(*g_symbol_table);
	printf("After substitution: %f\n", g_expr->eval());

    return 0;
}
