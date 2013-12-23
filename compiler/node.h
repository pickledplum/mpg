#ifndef NODE_HH
#define NODE_HH

#include <iostream>
#include <string>
#include <map>
class FSParam;

typedef std::map<std::string, FSParam*> SymbolTable;
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

class FSParam : public Terminal {
public:
	std::string name;
	FSParam( const std::string& name, float value );
	FSParam( const std::string& name );
	virtual float eval();
	virtual ~FSParam();
};

class BinaryOperator : public Expression {
public:
	char op;
	Node *lhs;
	Node *rhs;
	BinaryOperator(Node * lhs, char op, Node * rhs);
	virtual ~BinaryOperator();
	virtual float eval();
};

class UnaryFunction : public Expression {
public:
	std::string op;
	Node *expr;
	UnaryFunction(const std::string& op, Node* expr);
	virtual ~UnaryFunction();
	virtual float eval() ;
};

class BinaryFunction : public Expression {
public:
	std::string op;
	Node *lhs;
	Node *rhs;
	BinaryFunction(const std::string& op, Node* lhs, Node* rhs);
	virtual ~BinaryFunction();
	virtual float eval() ;
};

#endif