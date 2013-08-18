
=head1 NAME

Feedback - create feedback

=head1 RESPONSIBILITIES

Create messages like "foo created" or "created with compilation errors".

=head1 COLLABORATIONS

This is a library, not a class.

=cut


package Feedback;
use Misc;
use ConIo;
use Statement;
use Settings;
use strict;
use vars qw (
 @ISA
 $COMMANDS
);

@ISA = qw (
);

$COMMANDS = {};

#----------------------------------------
sub natureOfStatement {
	# get operation ("create"), objectType ("view")  and objectName ("foo")
	# This is needed for "show errors" and "View created." output.

	my ($string) = @_;
	$string =~ /^\s*
		# $1 = operation
		(create|alter|drop|truncate|grant|revoke|analyze|comment)

		# $2 ignore this
		(\s+or\ replace|\s+unique|\s+public)*

		# $3 ignore this too
		(\s+force)*
		\s+

		# $4 = object_type
		(procedure|function|table|sequence|package\s+body|package|trigger|view|index|user|[\w,]+\s+to|[\w,]+\s+from|session|database.*link|synonym|system|rollback.*segment|insert\s+on|update\s+on|delete\s+on|execute\s+on|all\s+on|select\s+on|on\s+table|on\s+column)
		\s+

		# $5 = schema (optional)
		(\w*\.)*

		# $6 = object name (may be quoted)
		([\"\w\$]+)*

	        # $7 $8 compile something
		#\s*(compile)*\s*(body)
	/ix;

	# print "natureOfStatement op=$1 type=$4 name=$6\n";
	# check if we found anything useful
	if ($1 && $4 && $6) {
		return
		  {
		   operation=>$1, 
		   objectType=>$4, 
		   objectName=>$6,
		   };
	}
	return 0;
}

# ----------------------------------------
sub past {
	# translate some verbs to past tense
	my ($verb) = @_;

	if ($verb =~ /create/i) { return "created"}
	if ($verb =~ /alter/i) { return "altered"}
	if ($verb =~ /drop/i) { return "dropped"}
	if ($verb =~ /truncate/i) { return "truncated"}
	if ($verb =~ /grant/i) { return "granted"}
	if ($verb =~ /revoke/i) { return "revoked"}
	if ($verb =~ /analyze/i) { return "analyzed"}
	if ($verb =~ /comment/i) { return "commented"}
}


# ----------------------------------------
sub feedbackOff {
	# return true if feedback is set to OFF (shortcut)
	my $rc = setting->get("feedback") eq 'OFF';
	return $rc;
	    
}
# ----------------------------------------
sub report {
	# print things like "Package created" or compilation errors warning

	my ($sql) = @_;

	return if feedbackOff();
	my $nature = $sql->nature or return;

	# we report only once
	if ($nature->{reported}) {
		$nature->{reported} = 1;
		return;
	}
	$nature->{reported} = 1;

	if ($nature->{operation} =~ /create|alter|drop|enable|disable|truncate|grant|revoke|analyze|comment/i) {

		my $name = initcap($nature->{objectName})." ";
		my $type = initcap($nature->{objectType})." ";
		my $operation = initcap($nature->{operation});

		# Sessions don't have names
		if ($type =~ /session/i) {
			$name="";
		} 
		print "$type$name".past($operation).".\n"
		  unless compilationErrors ($sql);
		return 1;
	} else {
		print "Statement processed\n";
		return 0;
	}

}

# ----------------------------------------
sub operationIs {
	my ($nature, $command) = @_;
	return uc("$nature->{operation} $nature->{objectType}") eq uc($command);
}
# ----------------------------------------
sub compilationErrorsSth {
	# return a sth to fetch user errors based on $nature
	my ($sql) = @_;

	my $nature = $sql->nature;
	my $relevantStatement = 
	  (
	   uc($nature->{objectType}) =~ 
	   /(
	     TRIGGER|
	     PROCEDURE|
	     FUNCTION|
	     PACKAGE|
	     PACKAGE BODY
	     )/ix
	   );
	return undef unless $relevantStatement;
	# for "alter package compile" 
	my $objectType;
	if (operationIs ($nature, "alter package")) {
		$objectType = $nature->{objectType}."%";
	} else {
		$objectType = $nature->{objectType};
	}

	my $statement = qq(
			   select 
				line,
				position,
				text,
				type,
				name
			   from user_errors
			   where name = upper('$nature->{objectName}')
			   and type like upper ('$objectType')
			   order by sequence, line
			   );
	return $sql->prepare($statement);
}

# ----------------------------------------
sub compilationErrors {
	# check for compilation errors + print warning if any
	my ($sql) = @_;

	if (my $sth = compilationErrorsSth ($sql)) {
		$sth->execute;
		if ($sth->fetchrow_arrayref) {
			print "Warning " .
			  $sql->nature->{objectType}." ". $sql->nature->{objectName} .
			    " created with compilation errors.\n";
			return 1;
		}
	return 0;
	}
}

# ----------------------------------------
sub handleWheneverSqlerror {
	# continue or not after sql and other errors
	if (setting->{sqlerror} eq 'EXIT') {
		exit;
	}
	if (setting->{sqlerror} eq 'ASK') {
		conOut "\nContinue ?";
		my $value =  conIn;
		unless ($value =~ /^(y|Y|j|J)/) {
			exit;
		}
		print "You're the boss.\n";
	}
}

# ----------------------------------------
sub formatError {
	my ($error) = @_;
	return unless ($error);
	#remove the DBI stuff so it looks more like sqlplus
	$error =~ s/\(DBD.*\)$//;
	return "\n".$error."\n";
}
# ----------------------------------------
sub printFetchError {
	my ($sth) = @_;
	print formatError($sth->errstr);
}
# ----------------------------------------
sub printError {
	# print dbh->errstr 
	my ($this) = @_;

	my $sql = $Senora::SESSION_MGR->mainSession;
	my $error = $sql->{dbh}->errstr;
	$error = formatError($error) or return;
	return if  $error =~ "no statement executing";
	if (setting->get("dropErrors") eq 'OFF') {
		if ($error =~ /^(ORA-00942|ORA-02443|ORA-04080|ORA-01434|ORA-01432|ORA-01418|ORA-02289|ORA-04043|ORA-02024)/m) {
			print "Object gone.\n" unless feedbackOff();
			return;
		}
	} else {
		print ("$error");
		handleWheneverSqlerror;
	}
}

# ----------------------------------------
sub printRowsSelected {
	return if feedbackOff
	my ($this, $rows) = @_;
	return unless setOn('feedback');
	if ($rows > 1) {
		print "\n$rows rows selected.\n" 
	} 

	if ($rows == 1) {
		print "\n$rows row selected.\n" 
	} 

	if ($rows == 0) {
		print "\nNo rows selected.\n" 
	}
}

1;
