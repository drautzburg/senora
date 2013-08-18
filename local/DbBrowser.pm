# --------------------------------------------------------------------------------  
# NAME
#   Dbbrowser - routines to walk the dabase
# RESPONSIBILITIES
#   Generate and execute queries. Build joins automagically.
#   Maintain a context ("where you are").
#
# COLLABORATIONS
#
# History
# --------------------------------------------------------------------------------  
# $log$
# --------------------------------------------------------------------------------  

=head1 NAME

	Dbbrowser - routines to walk the dabase

=head1 RESPONSIBILITIES
	Generate and execute queries. Build joins automagically.
	Maintain a context ("where you are").

=head1 COLLABORATIONS 
=cut 

package DbBrowser;
use strict;
use Settings;
use Plugin;
use SessionMgr qw(sessionNr sql);
use Misc;

use vars qw 
  (
   @ISA
   $COMMANDS
);

@ISA = qw (
	   Plugin
);
#------------------------------------------------------------
my $context = {
	       table => undef,
	       rowid => undef,
	       # join condition between two tables
	       parents => {},
	       children => {},
	       nonKeyColumns => {}
};
my $dbbTable = "SENORA_DBB";
#------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this = {};

	$this->{commands} = $COMMANDS;
	bless ($this, $class);
	return $this;
}

sub fetchNonKeyColumns {
	my ($table) = @_;

	# if we have the columns already ...
	$table = uc($table);
	my $columns = $context->{nonKeyColumns}->{$table};
	#return $columns if $columns;

	my $stmt = qq(
		select table_name, column_name
		from
			user_tab_columns ut
		where table_name = '$table'
		and not exists (
			select 1
			from $dbbTable
			where parent = ut.table_name
			and	p_column = ut.column_name
		)
		and not exists (
			select 1
			from $dbbTable
			where child = ut.table_name
			and	c_column = ut.column_name
			)
	);
	my $sth = sql->prepare($stmt);
	$sth->execute;
	my @columns;
	while (my $row = $sth->fetchrow_hashref) {
		# print "col = $row->{COLUMN_NAME}\n";
		push (@columns, $row->{COLUMN_NAME});
	}
	$context->{nonKeyColumns}->{$table} =\@columns;
	return \@columns;
}

sub addNonKeyColumns {
	my ($table, $query) = @_;
	$table = uc($table);
	my $columnArray = fetchNonKeyColumns($table);
	foreach my $col (@$columnArray) {
		my $columnName = "$table.$col";
		my $columnAlias = substr ("${table}_$col", -30);
		$query->addNewColumn("$columnName $columnAlias");
	}
}

# ------------------------------------------------------------
$COMMANDS->{dbbGui} =
  {
   alias => "",
   description => "start the Dbb Gui",
   man=>q(
	Put a more verbose description here. Indent by one tab.
	Can be omitted entirely.
BUGS
	none
),
   synopsis => 
   [
    [],
    ],
   routine => \&dbbGui,
   };

sub dbbGui {
	my ($this, $argv, $opts) = @_;

	# create an instance of the Wx::App-derived class
		my( $app ) = DbbGui->new();
		# start processing events
		$app->MainLoop();
}

# ------------------------------------------------------------
$COMMANDS->{dbbInit} =
  {
   alias => "",
   description => "initialize the constraintCache",
   man=>q(
	Put a more verbose description here. Indent by one tab.
	Can be omitted entirely.
BUGS
	You need SELECT ANY DICTIONARY granted
	The command takes forever when obj$ is not analyzed
),
   synopsis => 
   [
    [],
    ["-p", undef, "print cache"]
    ],
   routine => \&dbbInit,
   };

sub dbbInit {
	my ($this, $argv, $opts) = @_;
	print "Initializeing\n";
	;
	print "drop\n";
	sql->run ("drop table $dbbTable");
	print "create\n";
	sql->run (qq(
	create table senora_dbb as
		select
			parent.table_name parent,
			parent.column_name p_column,
			uc.constraint_name,
			child.table_name child,
			child.column_name c_column

		from 
			user_constraints uc,
			user_cons_columns child,
			user_cons_columns parent
		where
			1=1
		and	uc.constraint_name = child.constraint_name
		and	uc.r_constraint_name = parent.constraint_name
		and	parent.position = child.position
	));
	print "alter\n";
	sql->run ("alter table $dbbTable add (relationship char(1))");
	sql->run ("alter table $dbbTable add (hard char(1))");
	print "update\n";
	sql->run ("update $dbbTable set relationship = 'Y'");
	sql->run (qq(
	  	update $dbbTable t
		set relationship = 'N'
		where child in (
			select table_name
			from (
			select table_name, column_name
			from user_tab_columns
				minus 
			select child, c_column
			from $dbbTable
			)
		)
		));
	sql->run ("update $dbbTable set hard = 'N'");
	sql->run (qq(
	  	update $dbbTable t
		set hard = 'Y'
		where exists 
			(
				select 1
				from user_constraints uc
				where uc.constraint_name = t.constraint_name
			)
		));

	if ($opts->{p}) {
		sql->run ("select distinct parent, child from $dbbTable");
	}
}

# ------------------------------------------------------------
$COMMANDS->{dbbSetContext} =
  {
   alias => "",
   description => "set the initial context to start browsing",
   man=>q(
	Put a more verbose description here. Indent by one tab.
	Can be omitted entirely.
BUGS
	Mention known bugs here
),
   synopsis => 
   [
    ["where clause"],
    ["-t", "table", "the table"],
    ],
   routine => \&dbbSetContext,
   };

sub dbbSetContext {
	my ($this, $argv, $opts) = @_;
	my $where = "@$argv" eq "" ?  "where 1=1" : "where ";

	my $sth = sql->prepare ("select rowid x from $opts->{t} $where  @$argv and rownum < 3");
	$sth->execute;

	my $rows=0;
	my $table = uc ($opts->{t});
	my $lastrow;
	while (my $row = $sth->fetchrow_arrayref) {
		$rows++;
		$lastrow = $row;
	}
	if ($rows) {
		print "Ambigous context\n" if ($rows>1);

	      	$context->{table} = $opts->{t};
		$context->{rowid} = $lastrow->[0];
		dbbShowContext();

	} else {
		print "No row selected\n" unless $rows;
	}
}

# ------------------------------------------------------------
$COMMANDS->{dbbShowContext} =
  {
   alias => "",
   description => "show the current context",
   man=>q(
	Put a more verbose description here. Indent by one tab.
	Can be omitted entirely.
BUGS
	Mention known bugs here
),
   synopsis => 
   [
    [],
    ["-l", undef, "show full context"]
    ],
   routine => \&dbbShowContext
   };

sub dbbShowContext {
	my ($this, $argv, $opts) = @_;
	unless ($context->{table}) {
		print "No context set\n" ;
		return;
	}
	if ($opts->{l}) {
		dbbShowFullContext($context->{table}, $context->{rowid});
	} else {
		sql->run ("select * from $context->{table} where rowid = '$context->{rowid}'");		
	}

}

sub dbbAddParentQuery {
	my ($child, $query) = @_;

	my $parents = parentsOf($child);
		return unless $parents;
		foreach my $parent(@$parents) {
			$query = simpleJoin ($parent, $child, $query);
			# recurse. Cannot do connect by though
			dbbAddParentQuery ($parent, $query) unless ($parent eq $child);
		}
}


sub dbbShowFullContext {
	my ($child, $rowid) =  @_;
	my $query = DbbQuery->new();

	$query->addNewClause ("$child.rowid = '$rowid'");
	addNonKeyColumns($child, $query);
	$query->addNewTable ($child);

	dbbAddParentQuery($child, $query);
	my $sth = sql->prepare($query->asString);
	$sth->execute;
	my $columns = $sth->{NAME};
	my $n = 0;
	my $row = $sth->fetchrow_arrayref or return;
	while ($columns->[$n]) {
		printf ("%-32s%s\n", $columns->[$n], $row->[$n]);
		$n++;
	}
}

# ------------------------------------------------------------
$COMMANDS->{dbbJoin} =
  {
   alias => "",
   description => "generate SQL to join two tables",
   man=>q(
	Put a more verbose description here. Indent by one tab.
	Can be omitted entirely.
BUGS
	Mention known bugs here
),
   synopsis => 
   [
    [],
    ["-P", "parent", "the parent table"],
    ["-C", "child", "the child table"]
    ],
   routine => \&dbbJoin,
   };
#------------------------------------------------------------
sub dbbJoin {
	my ($this, $argv, $opts) = @_;
	my $query = DbbQuery->new();

	$query = simpleJoin ($opts->{P}, $opts->{C}, $query);
	$query->print();
}

#------------------------------------------------------------
sub simpleJoin {
	my ($parent, $child, $query) = @_;

	addNonKeyColumns($parent, $query);
	addNonKeyColumns($child, $query);

	$query->addNewTable ($parent);
	$query->addNewTable ($child);


	$parent = "'".uc($parent)."'";
	$child  = "'".uc($child)."'";
	my $stmt = qq(
	  	select * from $dbbTable
		where parent = $parent
		and   child = $child
	);
	my $sth = sql->prepare($stmt);
	$sth->execute;

	while (my $row = $sth->fetchrow_hashref) {
		$query->addNewClause ("$row->{PARENT}\.$row->{P_COLUMN} = $row->{CHILD}\.$row->{C_COLUMN}");
	}
	return $query;
}

#------------------------------------------------------------
sub parentQuery {
	my ($parent, $child, $query) = @_;
	#xxx
	addNonKeyColumns($parent, $query);
	$query->addNewTable ($parent);

	$parent = "'".uc($parent)."'";
	$child  = "'".uc($child)."'";
	my $stmt = qq(
	  	select * from $dbbTable
		where parent = $parent
		and   child = $child
	);
	my $sth = sql->prepare($stmt);
	$sth->execute;

	while (my $row = $sth->fetchrow_hashref) {
		$query->addNewClause ("$row->{PARENT}\.$row->{P_COLUMN} = $row->{CHILD}\.$row->{C_COLUMN}");
	}
	return $query;
}

#------------------------------------------------------------
sub parentsOf {
	my ($table) = @_;
	$table = uc($table);
	my $decode = "decode (hard, 'Y',1, 'N', 0, 0)";
	my $sth = sql->prepare (qq(
		select parent, sum($decode)
		from $dbbTable 
		where child = '$table' 
		group by parent
	        having sum($decode) > 0
	 ));
	$sth->execute;
	my $parents = [];
	my $rows=0;
	while (my $row = $sth->fetchrow_arrayref) {
		$rows++;
		push (@$parents, $row->[0]);
	}
	return $parents if $rows;
	return 0;
}

#------------------------------------------------------------
#------------------------------------------------------------
package DbbQuery;
use strict;
use Settings;
use Plugin;
use SessionMgr qw(sessionNr sql);
use Misc;

#------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this = {
		    columns => [],
		    tables => [],
		    clauses => []
		   };

	bless ($this, $class);
	return $this;
}

#------------------------------------------------------------
sub add {
	my ($this, $sectionName, $what) = @_;
	my $section = $this->{$sectionName};
	push (@$section, $what);
}

#------------------------------------------------------------
sub contains {
	my ($this, $sectionName, $what) = @_;
	my $section = $this->{$sectionName};
	foreach my $element (@$section) {
		return 1 if $element =~ /$what/i;
	}
	return 0;
}

#------------------------------------------------------------
sub addNew {
	my ($this, $sectionName, $what) = @_;
	$this->add($sectionName, $what) unless ($this->contains ($sectionName, $what));
}

#------------------------------------------------------------
sub addNewColumn {
	my ($this, $what) = @_;
	$this->addNew("columns", $what);
}

#------------------------------------------------------------
sub addNewTable {
	my ($this, $what) = @_;
	$this->addNew("tables", $what);
}

#------------------------------------------------------------
sub addNewClause {
	my ($this, $what) = @_;
	$this->addNew("clauses", $what);
}

#------------------------------------------------------------
sub sectionAsString {
	my ($this, $sectionName, $separator) = @_;
	my $section = $this->{$sectionName};
	my $result = undef;
	my $n = @$section;
	foreach my $element (@$section) {
		$n--;
		$result .= "\t$element" ;
		$result .= ($n==0)? "\n" : " $separator\n";
	}
	return $result;
}

#------------------------------------------------------------
sub asString {
	my ($this) = @_;
	my $result = "Select\n"; 
	$result .= $this->sectionAsString("columns", ",");
	$result .= "From\n"; 
	$result .= $this->sectionAsString("tables", ",");
	$result .= "Where\n"; 
	$result .= $this->sectionAsString("clauses", "and");
	return $result;
}

#------------------------------------------------------------
sub print {
	my ($this) = @_;
	print $this->asString;
}

package DbbGui;
use Wx;

# every program must have a Wx::App-derive class


use strict;
use vars qw(@ISA);

@ISA=qw(Wx::App);

# this is called automatically on object creation
sub OnInit {
  my( $this ) = @_;

  # create new MyFrame
  my( $frame ) = MyFrame->new( "Database browser",
			       Wx::Point->new( 50, 50 ),
			       Wx::Size->new( 800, 600 )
                             );

  # set it as top window (so the app will automatically close when
  # the last top window is closed)
  $this->SetTopWindow( $frame );
  # show the frame
  $frame->Show( 1 );

  1;
}

package MyFrame;

use strict;
use vars qw(@ISA);

@ISA=qw(Wx::Frame);

use Wx::Event qw(EVT_MENU);
use Wx qw(wxBITMAP_TYPE_ICO wxMENU_TEAROFF);

# Parameters: title, position, size
sub new {
  my( $class ) = shift;
  my( $this ) = $class->SUPER::new( undef, -1, $_[0], $_[1], $_[2] );

  # load an icon and set it as frame icon
  $this->SetIcon( Wx::GetWxPerlIcon() );

  # create the menus
  my( $mfile ) = Wx::Menu->new( undef, wxMENU_TEAROFF );
  my( $mhelp ) = Wx::Menu->new();

  my( $ID_ABOUT, $ID_EXIT ) = ( 1, 2 );
  $mhelp->Append( $ID_ABOUT, "&About...\tCtrl-A", "Show about dialog" );
  $mfile->Append( $ID_EXIT, "E&xit\tAlt-X", "Quit this program" );

  my( $mbar ) = Wx::MenuBar->new();

  $mbar->Append( $mfile, "&File" );
  $mbar->Append( $mhelp, "&Help" );

  $this->SetMenuBar( $mbar );

  # declare that events coming from menu items with the given
  # id will be handled by these routines
  EVT_MENU( $this, $ID_EXIT, \&OnQuit );
  EVT_MENU( $this, $ID_ABOUT, \&OnAbout );

  # create a status bar (note that the status bar that gets created
  # has three panes, see the OnCreateStatusBar callback below
  $this->CreateStatusBar( 2 );
  # and show a message
  $this->SetStatusText( "Ready", 1 );

  $this;
}

# this is an addition to demonstrate virtual callbacks...
# it ignores all parameters and creates a status bar with three fields
sub OnCreateStatusBar {
  my( $this ) = shift;
  my( $status ) = Wx::StatusBar->new( $this, -1 );

  $status->SetFieldsCount( 3 );

  $status;
}

# called when the user selects the 'Exit' menu item
sub OnQuit {
  my( $this, $event ) = @_;

  # closes the frame
  $this->Close( 1 );
}

use Wx qw(wxOK wxICON_INFORMATION wxVERSION_STRING);

# called when the user selects the 'About' menu item
sub OnAbout {
  my( $this, $event ) = @_;

  # display a simple about box
  Wx::MessageBox( "Generic database browser.\n" ,
		  "About Dbb", wxOK | wxICON_INFORMATION,
		  $this );
}

