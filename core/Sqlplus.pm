=head1 NAME

Sqlplus - Provide Sqlplus commands and enhancements

=head1 RESPONSIBILITIES

Main wrapper around various auxilary packages to provide sqlplus
funcionality and things we always missed in sqlplus.

=head1 COLLABORATIONS

=cut

# $Id: Sqlplus.pm,v 1.2 2012/03/16 17:47:46 draumart Exp $

package Sqlplus;
use Feedback;
use SessionMgr;
use Settings;
use Setting;
use strict;
use Plugin;
use Text::Wrap;
use Getopt::Long;
use Cwd;
use Misc;
Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");


use vars qw (
	     @ISA
	     $COMMANDS
);

@ISA = qw (
	   Plugin
);

$COMMANDS = {};

# We need to put some commands here because the place where they
# belong is not a Plugin. Reading settings is not a problem, but only
# Plugins have commands.

newSetting("feedback", "ON", "OFF", 
		    "Report command execution or not")->{alias}='foo';
newSetting("heading", "ON", "OFF", "Print heading or not");
newSetting("colsep", "ON", "OFF|aChar", "Set column separator");
newSetting("pagesize", "0", "aNumber", "Break pages");
newSetting("dropErrors","ON","OFF","OFF replaces ORA-00942 etc with 'object gone'");
newSetting("sqlerror","CONTINUE","EXIT|ASK","what should happen on sqlerror");
newSetting("rowLimit", "0", "aNumber", "Limit number of returned rows");

my $pushPopHelp = qq(
	The push and pop commands solve the problem when a script is
	called which changes a setting. Doing a push before the script
	is called and a pop after the script returns will restore the
	original settings. This concerns things like pagesize and the
	like.);

# ------------------------------------------------------------

## =========================== Initializing ===========================
# ------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this;
	# ----------------------------------------

	$this->{commands} = $COMMANDS;
	$this->{out} = undef;
	bless ($this, $class);
	# must pass myself so TIEHANDLE does not create a new object
	tie *STDOUT, $class, $this;

	return $this;
}


# ------------------------------------------------------------

## =========================== finding objects ===========================
sub qualify {
	# turn object or (owner, object) into (owner, object) 

	my ($s1, $o2) = @_;
	my $schema;
	my $object;

	if ($o2) {
		$schema = uc ("'".$s1."'");
		$object = uc ("'".$o2."'");

	} else {
		$schema = "user";
		$object = uc ("'".$s1."'");
	}
	return ($schema, $object);
}

# ------------------------------------------------------------
sub findObject {
	# find non-synonym object in schema. Input must be uppercase and properly quoted

	my ($sql, $schema, $object) = @_;
	my $type;

	my $statement = qq (
		select object_type
		from all_objects
		where object_name = $object
		and owner = $schema
		and object_type != 'SYNONYM'
	);
	$sql->prepare ($statement) or print "fail\n";
	if ($sql->execute) {
		$type = $sql->{sth}->fetchrow_arrayref;
		if ($type) {
			return ($schema, $object, $type->[0]);
		} 
	}

	return undef;
}

# ------------------------------------------------------------

## =========================== describe ===========================
sub resolveSynonym {
	# get name and owner of the real object of a synonym
	my ($sql, $owner, $object) = @_;

	my $statement = qq<
		select 
			    table_name, 
			    table_owner, 
		  	    owner,
			    db_link
		from all_synonyms
		where synonym_name = $object
		and (
			owner = $owner
			or (
				owner = 'PUBLIC'
				and not exists (
				select 1
				from all_objects
				where object_name=$object
				and owner = $owner
				and object_type != 'SYNONYM'
				)
			)
		)
		order by decode (owner, 'PUBLIC',2,1)

	>;
	$sql->prepare ($statement);

	return 0 unless $sql->execute;
	my $result = $sql->{sth}->fetchrow_arrayref;
	return 0 unless $result;
	
	print "realname = $result->[1].$result->[0]\n";
	if (my $link=$result->[3]) {
		print "Warning: cannot look beyond db_link $link yet.\n";
		return 0;
	}

	return ("'$result->[1]'", "'$result->[0]'");

}

# ------------------------------------------------------------
sub realname {
	# return (schema, object, type) even if input is unqualified
	# or a synonym

	my ($this, $sql, $s1, $o2) = @_;

	my $schema,
	my $object;
	my $type;
	my $link;

	# ----------------------------------------
	# get the schema right
	($schema, $object) = qualify ($s1, $o2);

	my @schemaObjectLink;
	# resolve synonym (for synonyms ... max 8 levels)
	for (my $i=0; $i<8; $i++) {
		@schemaObjectLink = ($schema, $object, $link);
		($schema, $object, $link) = resolveSynonym($sql, $schema, $object);
		# print "object=$schema $object $link\n";
		last unless $object;
	}

	my @schemaObjectType = findObject ($sql, $schemaObjectLink[0], $schemaObjectLink[1]);
	return @schemaObjectType if $schemaObjectType[0];

	return undef;
}

# ------------------------------------------------------------
sub resolveSynonym2 {
	# get name and owner of the real object of a synonym
	my ($sql, $obj) = @_;

	if ($obj->{type} !~ "'SYNONYM'") {
		return 0;
	}
	my $atTheLink = "@".$obj->{db_link} if $obj->{db_link}; 

	my $statement = qq(
		select 
			    table_name, 
			    table_owner,
			    db_link,
			    decode (owner, 'PUBLIC',0,1) pref
		from all_synonyms$atTheLink
		where synonym_name = $obj->{name}
		and ( owner = $obj->{owner} or owner = 'PUBLIC')
		order by pref
	);
	$sql->prepare ($statement);
	$sql->execute || return 0;
	my $result = $sql->{sth}->fetchrow_arrayref;
	if (!$result) { return 0;}

	print "realname = $result->[1].$result->[0]\n";
	$obj->{name} = $result->[0];
	$obj->{owner} = $result->[1];
	$obj->{db_link} = $result->[2];
	$obj->{type} = undef;

	# nothing we can do if it is a remote object
	if ($obj->{db_link}) {return 2;}

	
}

# ------------------------------------------------------------
sub describeTable {
	# describe a table or view
	my ($sql, $schema, $object, $link) = @_;

	my $stmnt = qq(
		select 
			co.column_name "Name", 
			co.nullable "Null?", 
			data_type||'('||nvl(data_precision,data_length)||')' Type
		from 
			all_tab_columns$link co
		where 
			    co.table_name = upper($object)
			    and co.owner = upper($schema)
		order by column_id
	);

	$sql->run($stmnt);
}
# ------------------------------------------------------------
sub describeTableWithComments {
	# describe a table or view
	my ($sql, $schema, $object, $link) = @_;

	# get the table comment
	my $sth = sql->prepare (qq(
		select table_name, comments 
		from all_tab_comments 
		where
			table_name = upper($object)
			and owner = upper($schema)
	));
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref()) {
		my $comment = $row->[1];
		$comment =~ s/^/   /mg;
		print "\n$row->[0]\n$comment" unless $row->[0] eq ""
	}

	print "\n";
	my $stmnt = qq(
		select 
			co.column_name "Name", 
			co.nullable "Null?", 
			data_type||'('||nvl(data_precision,data_length)||')' Type,
			comments
		from 
			all_tab_columns$link co,
			All_COL_Comments cc
		where 
			    co.table_name = upper($object)
			    and co.owner = upper($schema)
			    and co.table_name = cc.table_name
			    and co.column_name = cc.column_name
			    and cc.owner = co.owner
		order by column_id
	);

	$sql->run($stmnt);
	print "\n";
}

# ------------------------------------------------------------
sub describePackage {
	my ($sql, $schema, $object, $link) = @_;
	my $stmt = qq(
		select 
			decode(min(position),0,'FUNCTION','PROCEDURE') type, 
			object_name, 
			decode(min(position),0,'RETURN '||min(data_type), '') return ,
		        nvl(overload,-1)
		from all_arguments$link
		where package_name = $object 
		group by object_name, overload
	);
	my $sth = $sql->prepare($stmt);
	$sth->execute;
	my $row;
	while ($row = $sth->fetchrow_arrayref) {
		print "\n$row->[0] $row->[1] $row->[2] \n";
		my $member = uc ("'".$row->[1]."'");
		$stmt = qq(
		select
			argument_name,
			data_type
			|| decode (data_length, NULL, '', '('||data_length||')') type,
			in_out
		from
			all_arguments
		where
			package_name = $object
		and	object_name = $member
		and 	position > 0
	        and     nvl(overload,-1) = $row->[3]
		order by
			overload,
			sequence
		);
		$sql->run($stmt);
	}
}

sub describePackageShort {
	my ($sql, $schema, $object, $link) = @_;

	my $stmt = qq(
		select
			package_name||'.' package,
			object_name,
			argument_name,
			in_out,
			data_type,
			default_value
		from all_arguments
		where package_name like 'SEQU%'
		order by object_name, position
	);

	my $sth = $sql->prepare($stmt);
	$sth->execute;

	my $lastFunc=undef;
	while (my $row = $sth->fetchrow_arrayref) {
		my $func = $row->[0].$row->[1];
		if ($lastFunc ne $func) {
			print Misc::initcap(")\n$func (");

		} else {
			print ", ";
		}
		print Misc::initcap($row->[2]." ".$row->[3]." ".$row->[4]);

		$lastFunc=$func;
	}
	print ")\n";
}

# ------------------------------------------------------------
$COMMANDS->{describe} =
  {
   alias => "desc|d",
   description => "describe a table or command",
   synopsis => [
		["object"],
		["-l", undef, "show comments on tables and views"]
		],
   routine => \&describe,
   };
sub describe {
	my ($this, $argv, $opts) = @_;

	my $sql = $Senora::SESSION_MGR->{mainSession};

	my ($schema, $object) = split ('\.',$argv->[0]);

	# must use subeselect hence the $sql=...
	($schema, $object, my $type) = $this->realname($sql, $schema, $object);

	if (!$type) {
		print "No such object\n";
		return;
	}
	if ($type =~ /PACKAGE/) {
		describePackage ($sql, $schema, $object);
		return;
	}
	if ($type =~ /(TABLE|VIEW)/ && ! $opts->{l}) {
		describeTable ($sql, $schema, $object);
		return;
	}
	if ($type =~ /(TABLE|VIEW)/ && $opts->{l}) {
		describeTableWithComments ($sql, $schema, $object);
		return;
	}

	print "Cannot describe a $type\n";


}

# ------------------------------------------------------------


## =========================== formatting ===========================
$COMMANDS->{column} = 
  {
   alias => "",
   description => "set/list column format",
   synopsis => [
		["name format ann"],
		],
   routine => \&column,
   };
sub column {
	my ($this, $argv, $opts) = @_;

	if ($this->noArgs (@_)) {
		columnFormat->printAll;
		return;
	}
	my ($name, $foo, $format) = @$argv;
	if (uc($foo) =~ /FORMAT/) {
		columnFormat->set(uc($name),$format);
		return;
	}
	print "Don't know how to handle $foo\n";
}

# ------------------------------------------------------------
$COMMANDS->{define} =
  {
   alias => "",
   description => "define a value or list definitions",
   synopsis => [
		["name=value"],
		[-a, undef, "print entire stack"],
		],
   routine => \&defineHere,
   };
sub defineHere {
	my ($this, $argv, $opts) = @_;

	# called without arg => list defines
	unless (@$argv) {
		if ($opts->{a}) {
			settings->printAll();			
		} else {
			define->printAll();			
		}
		return;
	}

	my $definition = "@$argv";
	if ($definition =~ /(\w*) *= *(.*)/) {
		my $name=$1;
		my $values=$2;
		$values = Bind::ampersandReplace($values);
		define->set($name, $values);
	} else {
		print "define requires equal sign (=)\n";
	}
}

# ------------------------------------------------------------

$COMMANDS->{"dateFormat"} = 
  {
   alias => "df",
   description => "shortcut for 'alter session set NLS_DATE_FORMAT'",
   synopsis => [
		[],
		["-L", undef, "set to dd.mm.yyyy hh24:mi:ss"],
		["-l", undef, "set to dd.mm.yy hh24:mi"],
		["-s", undef, "set to dd.mm. hh24:mi:ss"],
		["-S", undef, "set to dd.mm. hh24:mi"],
		],
   routine => \&dateFormat,
   };
sub dateFormat {
	my ($this, $argv, $opts) = @_;
	my $dateFormat;
	if ($opts->{l}) {$dateFormat = "dd.mm.yy hh24:mi"};
	if ($opts->{L}) {$dateFormat = "dd.mm.YYyy hh24:mi:ss"};
	if ($opts->{"s"}) {$dateFormat = "dd.mm hh24:mi:ss"};
	if ($opts->{"S"}) {$dateFormat = "dd.mm hh24:mi"};

	sql->run ("alter session set nls_date_format = '$dateFormat'");
}

# ----------------------------------------
$COMMANDS->{"date"} = 
  {
   description => "print current date and time of server",
   synopsis => [
		[],
		],
   routine => \&date,
   };
sub date {
	my ($this, $argv, $opts) = @_;
	my $dateFormat;
	my $now = sql->runArrayref (qq(
		select to_char(sysdate,'Day Month dd yyyy hh24:mi:ss') "now is" 
		from dual
	));
	print "$now->[0]->[0]\n";
}


1;




## =========================== misc user commands ===========================
$COMMANDS->{pushSetting} =
  {
   alias => "push",
   description => "push current settings to stack",
   synopsis => [
		[],
		],
   man => $pushPopHelp,
   routine => sub {settings->push}
   };

# ------------------------------------------------------------
$COMMANDS->{popSetting} =
  {
   alias => "pop",
   description => "pop current settings from stack",
   synopsis => [
		[],
		],
   man => $pushPopHelp,
   routine => sub {settings->pop}
   };

$COMMANDS->{"show errors"} =
  {
   alias => "show err",
   description => "show compilation errors",
   synopsis => [
	       [],
	       ["-v", undef, "list source code too"],
		],
   routine => \&showErrors,
   };
sub showErrors {
	my ($this, $argv, $opts) = @_;
	my $sql = $Senora::SESSION_MGR->mainSession;
	my $nature = $sql->nature;

	if (!$nature || !($nature->{objectName})) {
		print "No object to report errors of.\n";
		return;
	}

	my $sth = Feedback::compilationErrorsSth ($sql);
	return unless $sth;
	$sth->execute;
	my $rows = 0;
	my $lastLine = "no set";

	# xxx needs beautification:
	while (my $row = $sth->fetchrow_arrayref) {
		if ($rows==0) {
			print "Errors for $nature->{objectType} "
			  ."$nature->{objectName}:\n";
		}

		$Text::Wrap::columns=70;
		my $text = $row->[2];
		$text =~ s/\n/  /g;
		my $position="$row->[0]/$row->[1]";
		if ($position =~ $lastLine) {
			printf ("%-8s%s\n", '', $text)
		} else {
			if ($opts->{v}) {
				print "-------------------------------------\n";
				my $source = qq(
					select line,text from user_source 
					where type = '$row->[3]'
					and name = '$row->[4]'
					and line between $row->[0]-3 and $row->[0]+1
						);
				my $sts= $sql->prepare($source);
				$sts->execute;
				while (my $srow=$sts->fetchrow_arrayref) {
					printf ("%-5d:%s", $srow->[0], $srow->[1]);
				}
				print "\n";
			}
			printf ("%-8s%s\n", $position, $text)
		}

		$lastLine = $position;
		$rows++;
	}
	if ($rows == 0) {
		print "No errors.\n";
	}
}

# ------------------------------------------------------------
$COMMANDS->{cat} =
  {
   description => "synonym for select * from where rownum < 3000",
   synopsis => [
		["table [extra clause]"],
		],
   routine => \&cat,
   };


# ------------------------------------------------------------
$COMMANDS->{head} =
  {
   description => "select first rows from table",
   synopsis => [
		["table [extra clause]"],
		["-n", "rows", "set number or rows to show (25)"]
		],
   routine => \&head,
   };


sub cat {
	my ($this, $argv, $opts) = @_;

	my $table = $argv->[0] or return;

	my $rows = $opts->{n} ? $opts->{n} : 3000;
	my $rowClause = "and rownum <= $rows";


	# process additional cmdline clause
	splice (@$argv,0,1);
	my $clause = "@$argv";
	$clause =~ s/^where/and/i;

	sql->run(qq(
			   select t.* from $table t
			   where 1=1
			   $clause
			   $rowClause 
			   )
		 );
}

sub head {
	my ($this, $argv, $opts) = @_;
	$opts->{n} = 25 unless $opts->{n};
	cat ($this, $argv, $opts);

}

# ------------------------------------------------------------
$COMMANDS->{grep} =
  {
   alias => "",
   description => "show rows that match pattern",
   synopsis => [
		["pattern table"],
		["-C", "clause", "add additional 'where ...' clause"],
		["-v", undef, "show rows that don't match pattern"],
		["-i", undef, "ignore case"],
		],
   man=>q(
	Finally ... This does just what you´d expect: it looks for the
	pattern anywhere in the table. It combines all columns to a
	single big column.

	You can specify an extra where clause as in
	  grep LOCK Stmt_Audit_Option_Map where option#<100
BUGS
	Does not respect 'set ddView' 	
   ),
   routine => \&grep,
   };
sub grep {
	my ($this, $argv, $opts) = @_;

	my $pattern = $argv->[0] or return;
	$pattern = "\%$pattern\%";
	my $table = uc($argv->[1]) or return;
	my $not = $opts->{v} ? " not " : "";


	# get the columns of the table
	my $statement = qq (
		select column_name
		from all_tab_columns
		where upper(table_name) = '$table'
		and data_type != 'LONG'
	);

	my $sth = sql->prepare($statement);
	my $sth = sql->execute;
	my $columns="";
	while (my $row = $sth->fetchrow_arrayref) {
		$columns .= $row->[0] .'||'
	}
	$columns =~ s/\|\|$//;
	if (!$columns) {
		print "can't figure out columns for table '$table'.\n";
		return;
	}

	# process additional cmdline clause
	my $clause = $opts->{C};
	$clause =~ s/^where/and/i;


	my $statement;
	if ($opts->{i}) {
		$statement = qq<
			   select * from $table
			   where upper($columns) $not like upper('$pattern') $clause
			   >;
	} else {
		$statement = qq<
			   select * from $table
			   where $columns $not like '$pattern' $clause
			   >;
	}
	sql->run($statement);
}



# ------------------------------------------------------------
$COMMANDS->{list} = 
  {
   alias => "l",
   description => "list last sql/plsql statement",
   synopsis => [
		],
   routine => \&list,
   };
sub list {
	my ($this, $argv, $opts) = @_;

	my $history = $Senora::HISTORY;
	my $stmt = $history->{lastSql} or return;
	$stmt->list;
}

# ------------------------------------------------------------
$COMMANDS->{edit} = 
  {
   alias => "ed",
   description => "edit file or last sql/plsql statement",
   synopsis => [
		["filename"],
		["-n","historyItem","edit + run history item"],
		],
   routine => \&edit,
   };
sub edit {
	my ($this, $argv, $opts) = @_;
	my $fdStack = $Senora::MAINLOOP->{fdStack};
	my $filename = $argv->[0];

	if ($filename) {
		# with explicit filename only edit
		callEditor($filename);
	} else {
		# edit last statement and run it
		($filename = saveStatement($opts->{n})) or return;
		callEditor ($filename);
		$fdStack->readOpenFile("$filename");
	}
}

# ------------------------------------------------------------
sub saveStatement {
	my ($historyItem) = @_;

	my $history = $Senora::HISTORY;
	my $stmt;
	if ($historyItem) {
		$stmt = $history->getItemNumbered($historyItem);
	} else {
		$stmt = $history->{lastSql};		
	}

	my $filename = "afiedt.buf";
	unless ($stmt) {
		print "No statement to edit.\n";
		return undef;
	}
	$stmt->save($filename);
	return $filename;
}

# ------------------------------------------------------------
sub callEditor {
	my ($filename) = @_;
	my $fdStack = $Senora::MAINLOOP->{fdStack};

	# add .sql to unqualified filenenames except "-"
	$filename .= ".sql" unless ($filename =~ /(\.|\-)/i);

	my $editor = define->get('_editor');
	my $term = "";

	# guess the default editor and if an xterm is needed
	unless ($editor) {
		if ($^O  =~ /win/i) { $editor = "notepad";}
		if ($^O  =~ /linux/i) { 
			$editor = "vi";
			# xxx fixme
#			if (!$fdStack->isaTty or $ENV{TERM} eq "emacs" ) {
				$term = "xterm -e";
#			}
		}
	}
	system "$term $editor $filename";
}

# ------------------------------------------------------------
$COMMANDS->{"show user"} = 
  {
   alias => "id",
   description => "show the current username",
   synopsis => [
		],
   routine => \&showUser,
   };
sub showUser {
	my ($this, $argv, $opts) = @_;
	print "user is ". sql->identify."\n";
}

# ------------------------------------------------------------
$COMMANDS->{"spool"} = 
  {
   alias => "spo",
   description => "write to file",
   synopsis => [
		["file"]
		],
   routine => \&spool,
   };
sub prompt {
	my ($this, $argv, $opts) = @_;
	my $args = "@$argv";
	
	print "$args\n";
}


# ----------------------------------------
$COMMANDS->{"chop"} = 
  {
   alias => "",
   description => "delete from (large) table in chunks",
   synopsis => [
		["table where ..."],
		["-n", 1000, "chunk size"],
		["-i", 1000, "max iterations"],
		["-q", undef, "be quiet"]
		],
   man => q(
	Chop is useful, when you want to delete all rows from a large
	table, you cannot use truncate and you expect rollback
	trouble.

	It runs a series (-i) of delete statements, where each
	statement deletes some (-n) rows at a time and commits.

	You can specify an additional where cluase, as in
	    purge myTable where key=23
   ),

   routine => \&purge,
   };
sub purge {
	my ($this, $argv, $opts) = @_;
	my $table = "$argv->[0]";
	shift @$argv;
	my $where = "@$argv";
	$where =~ s/;[ 	]*$//;

	my $sql = $Senora::SESSION_MGR->mainSession;

	my $chunkSize = $opts->{n} ? $opts->{n} : 1000;
	my $and = $where ? "and" : "where";

	my $cmd = "
		delete --+ PARALLEL (t, 4)
		from $table t  $where
		$and rownum <= $chunkSize
	";

	my $iterations = $opts->{i} ? $opts->{i} : 1000;
	my $rows=1;
	while ($rows>0 and $iterations>0) {
		my $sth=$sql->prepare($cmd);
		if ($sth) {
			$rows = $sth->execute;
			return unless $rows > 0;
			my $ROWS = $rows == 1 ? "row" : "rows";
			print "($iterations) $rows $ROWS deleted from $table.\n" unless $opts->{"q"};
			$iterations--;
			$sql->run("commit");
		} else {
			return;
		}
	}
}


# ----------------------------------------
$COMMANDS->{"count"} = 
  {
   alias => "",
   description => "count rows in several tables at once",
   synopsis => [
		["pattern"],
		],
   man => q(
	This command counts the rows of tables that match the pattern
	and prints them. Pattens is specified as 'type/name' as usual,
	so table/*foo* gives you all tables whose names contain 'foo'.

	As opposed to a select count(*) from you can check several
	tables at a time.
BUGS
	Does not respect set ddView. Always looks at 'user_objects'.
	Does not allow to count user_objects
   ),
   routine => \&count,
   };
sub count {
	my ($this, $argv, $opts) = @_;
	my $sql = $Senora::SESSION_MGR->mainSession;
	my ($typePattern, $namePattern) = processPattern($argv);
	#$typePattern="'TABLE'" if "$typePattern" eq "'%'";

	my $sth_obj = $sql->prepare (qq(
		select object_name, subobject_name
		from user_objects
		where object_type like $typePattern
		and object_name like $namePattern
		and object_type in ('TABLE','VIEW','SYNONYM', 'TABLE PARTITION')
	));
	$sth_obj->execute;
	while (my $row=$sth_obj->fetchrow_arrayref) {
		my $object = $row->[0];
		my $partit = $row->[1];
		my $partitionClause;
		if ($partit) {
			$partitionClause = " partition($partit)";
		}
		my $sth=$sql->prepare(" 
				select '$object $partit', count(*) 
				from $row->[0] $partitionClause");
		$sth->execute;
		while (my $row=$sth->fetchrow_arrayref) {
			printf ("%-32s %9d\n", initcap($row->[0]), $row->[1]);
		}
	}
}
# ----------------------------------------

## =========================== Spool ===========================
sub spool {
	my ($this, $argv, $opts) = @_;
	my $file = $argv->[0] or return;

	if (uc($file) eq "OFF") {
		if ($this->{out}) {
			$this->{out}->close;
			$this->{out} = undef;
		} else {
			print ("Not spooling currently\n");
		}
		return;
	}
	if ($file !~ /\./) {
		$file .= ".lst";
	}

	
	my $fd = FileHandle->new(">$file");
	$this->{out} = $fd;
}

# ----------------------------------------
# the SPOOL redirections:
sub TIEHANDLE {
	my ($class, $myself) = @_;
	open REAL_STDOUT, ">&STDOUT" or die;
	return $myself;
}
sub PRINT {
	my ($this, $value) = @_;
	print REAL_STDOUT "$value";
	flush REAL_STDOUT;

	my $fd = $this->{out};
	print $fd $value if $fd;
}
sub PRINTF {
	my ($this, @rest) = @_;
	printf REAL_STDOUT @rest;
	flush REAL_STDOUT;

	my $fd = $this->{out};
	printf $fd @rest if $fd;

}
# ----------------------------------------
$COMMANDS->{"set serveroutput"} = 
  {
   alias => "so",
   description => "get dbms_output messages",
   synopsis => [
		["on|off"],
		["-b", "bufsz", "set the buffer size"]
		],
   routine => \&setServeroutput,
   };

## =========================== serveroutput ===========================
sub setServeroutput {
	my ($this, $argv, $opts) = @_;
	return unless connected("msg");
	my $mode = $argv->[0];

	if (uc($mode) eq "ON") {
		sql->{dbh}->func( 1000000, 'dbms_output_enable' );
		sql->serveroutput(1);
		return;
	}
	if (uc($mode) eq "OFF") {
		my $sth = sql->prepare
		  (qq(
		      begin dbms_output.disable; end;
		      ));
		$sth->execute;
		sql->serveroutput(0);
		return;
	}

	# report ON/OFF status if no parameters
	if (sql->serveroutput()) {
		print "serveroutput is on\n";
	} else {
		print "serveroutput is off\n";
	}
}
sub getServeroutput {
	return unless connected;
	return unless (sql->serveroutput());

	my $sql = $Senora::SESSION_MGR->mainSession;
	my @text;

	if ($sql) {
		my $dbh = $sql->{dbh};
		@text = $dbh->func( 'dbms_output_get' );
	}
	foreach my $line (@text) {
		print "$line\n"
	}
}


# ----------------------------------------
$COMMANDS->{"execute"} = 
  {
   alias => "exec",
   description => "run a single line PL/SQL",
   synopsis => [
		["psql"],
		],
   routine => \&plsqlExec,
   };

sub plsqlExec {
	my ($this, $argv, $opts) = @_;
	my $plsql = "@$argv";
	
	my $sql = $Senora::SESSION_MGR->mainSession;
	$sql->run("BEGIN $plsql; END;", "plsql");
}

# ----------------------------------------
$COMMANDS->{"prompt"} = 
  {
   alias => "",
   description => "echo arguments",
   synopsis => [
		["args"],
		],
   routine => \&prompt,
   };
