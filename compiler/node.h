#ifndef NODE_HH
#define NODE_HH

#include <iostream>
#include <string>
#include <map>
class Identifier;

typedef std::map<std::string, Identifier*> SymbolTable;
void print_table( const SymbolTable & table );
		
class Node {
public:
	virtual ~Node();
	virtual float eval() = 0;
};
class Expression : public Node {
public:
	virtual ~Expression();
	virtual float eval() = 0;
};
class Terminal : public Node {
public:
	float value;
	Terminal();
	Terminal(float value);
	virtual ~Terminal();
	void setValue( float value );
	virtual float eval() = 0;
};
class Numeric : public Terminal {
public:
	Numeric( float value ) ;
	virtual float eval();
	virtual ~Numeric();
};

class Identifier : public Terminal {
public:
	std::string name;
	Identifier( const std::string& name, float value );
	Identifier( const std::string& name );
	virtual float eval();
	virtual ~Identifier();
};
class Literal : public Expression {
public:
	Terminal & term;
	Literal(Terminal& term);
	virtual float eval();
	virtual ~Literal();
};
class BinaryOperator : public Expression {
public:
	char op;
	Expression &lhs;
	Expression &rhs;
	BinaryOperator(Expression & lhs, char op, Expression & rhs);
	virtual ~BinaryOperator();
	virtual float eval();
};
class UnaryFunction : public Expression {
public:
	std::string op;
	Expression &expr;
	UnaryFunction(const std::string& op, Expression& expr);
	virtual ~UnaryFunction();
	virtual float eval() ;
};
class BinaryFunction : public Expression {
public:
	std::string op;
	Expression &lhs;
	Expression &rhs;
	BinaryFunction(Expression& lhs, const std::string& op, Expression& rhs);
	virtual ~BinaryFunction();
	virtual float eval() ;
};
#endif