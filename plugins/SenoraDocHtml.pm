# ------------------------------------------------------
# NAME
#	SenoraDocHtml - print html documentation
# RESPONSIBILITIES
#	The sole purpose of this plugin is to print a nicely
#	formatted html documentation
# COLLABORATIONS
#	Plugin and PluginMgr provide the raw data
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

SenoraDocHtml - print html documentation

=head1  RESPONSIBILITIES

The sole purpose of this plugin is to print a nicely
formatted html documentation

=head1  COLLABORATIONS

Plugin and PluginMgr provide the raw data
=cut


package SenoraDocHtml;
use strict;
use SessionMgr qw(sessionNr sql);
use Plugin;
use Settings;
use Text::Wrap qw(wrap $columns $huge);
use ConIo;
use Misc;

use vars qw (
	     @ISA
	     $COMMANDS
);

@ISA = qw (
	   Plugin
);

$COMMANDS={};
# ------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this = {
		    fd => undef
		    };
	# ----------------------------------------
	$this->{commands} = $COMMANDS;

	bless ($this, $class);

	return $this;
}

# ------------------------------------------------------------
$COMMANDS->{"senoraDoc"} =
  {
	  alias => "",
	  description => "print documentation",
	  synopsis => 
	    [
	     [],
	     ["-f", "filename", "print to this file"],
	     ],
   routine => \&senoraDoc,
    };

sub senoraDoc {
	my ($this, $argv, $opts) = @_;
	my $fd;
	if (my $filename = $opts->{f}) {
		$fd = FileHandle->new(">".$opts->{f});
	}
	unless ($fd) {
		print "cannot open file.\n";
		return;
	}
	$this->{fd} = $fd;
	$this->docSenora();
	$fd->close();
}

# ------------------------------------------------------------
sub docSenora {
	my ($this)=@_;
	my $pluginMgr = $Senora::PLUGIN_MGR or die "no plugin mgr";
	my $title="Senora users guide";
	$this->writeOut(
qq(
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0//EN"><HTML>
<HEAD>
<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=ISO-8859-1">
<TITLE>$title</TITLE>
</HEAD>
<H1>$title</H1>
<BODY BGCOLOR="#ffffff">
));

	foreach my $p ($pluginMgr->allPlugins) {
		$this->docPlugin($p);
	}

$this->writeOut(qq(
</BODY>
</HTML>
));
}

# ------------------------------------------------------------
sub docPlugin {
	my ($this, $plugin)=@_;
	my $pluginName = ref($plugin);

	$this->writeOut(
qq(
<p>
<table bgcolor="#0000A0" class="doc" width="100%" ><tr><td>
<font size="+2" face="Helvetica" color="white">
$pluginName
</font>
</td></tr></table>
));


	foreach my $cname ($plugin->allCommands) {
		my $cmd = $plugin->{commands}->{$cname};
		$cname = Plugin::noRegexp($cname);
		$this->docCommand($cname, $cmd);
	}
}


# ------------------------------------------------------------
sub docCommand {
	my ($this, $name, $cmd) = @_;
$this->writeOut (qq(
<p>
<table bgcolor="#C0C0FF" class="doc" width="100%" ><tr><td>
<font size="+1" face="Helvetica">
$name
</font>
</td></tr></table>
));

	$this->itemParagraph("NAME", "$name - $cmd->{description}");
	if (my $alias=$cmd->{alias}) {
		$alias =~ s/\|/ | /g;
		$this->itemParagraph("<br>ALIAS", $alias);
	}
	$this->itemParagraph("<br>DESCRIPTION", $cmd->{man}) if ($cmd->{man});
}

# ------------------------------------------------------------
sub docSynopsis {
	my ($this, $name, $synopsis) = @_;
$this->writeOut(qq(
<table >
     <colgroup>
        <col width="40">
        <col width="100">
        <col width="400">
        <col width="100">
     </colgroup>
    <tr>
      <td></td>
      <td>SYNOPSIS</td>
    </tr>
    <tr>
      <td></td>
      <td></td>
      <td>$name <i>$synopsis->[0]->[0]</i></td>
      <td></td>
    </tr>
</table>

<table >
     <colgroup>
        <col width="40">
        <col width="100">
        <col width="10">
        <col width="90">
        <col width="100">
     </colgroup>

));
	shift @$synopsis;
	foreach my $option(@$synopsis) {
$this->writeOut(qq(
    <tr>
      <td></td>
      <td></td>
      <td>$option->[0]</td>
      <td><i>$option->[1]</i></td>
      <td>$option->[2]</td>
      <td></td>
    </tr>
));
	}
$this->writeOut(qq(
</table>
));
}

# ------------------------------------------------------------
sub docMan {
	my ($this, $name, $man) = @_;
	$this->writeOut(qq(
<table >
     <colgroup>
        <col width="40">
        <col width="80">
        <col width="800">
        <col width="100">
     </colgroup>
    <tr>
      <td></td>
      <td>DESCRIPTION</td>
    </tr>
    <tr>
      <td></td>
      <td></td>
      <td>$man</td>
      <td></td>
    </tr>
</table>
));
}

# ------------------------------------------------------------
sub writeOut {
	my ($this, $txt) = @_;
	my $fd = $this->{fd};
	print $fd $txt;
}


# ------------------------------------------------------------
sub commandHeader {
	my ($this, $name, $alias, $descr) = @_;
	$this->writeOut(
qq(
<p>
<table bgcolor="#D0D0FF" class="doc" width="100%" ><tr><td>
<font size="+1" face="Helvetica">
$name
</font>
</td></tr></table>
));
}


# ------------------------------------------------------------
# formatting
# ------------------------------------------------------------
sub optionTableHeading {
	my ($this) = @_;
$this->writeOut(qq(
<table >
     <colgroup>
        <col width="40">
        <col width="80">
        <col width="800">
        <col width="80">
     </colgroup>
));}

# ------------------------------------------------------------
sub textTableHeading {
	my ($this) = @_;
$this->writeOut(qq(
<table >
     <colgroup>
        <col width="40">
        <col width="80">
        <col width="800">
        <col width="80">
     </colgroup>
));}

# ------------------------------------------------------------
sub formatParagraph{
	my($this, $paragraph) =@_;
	
}
# ------------------------------------------------------------
sub itemParagraph {
	my ($this, $item, $paragraph) = @_;
	# handle empty lines
	$paragraph =~ s/(^\s*$)+/<br><br>/mg;
	$paragraph =~ s/^(<br>)*//;
$this->writeOut(qq(
<table border="0" style="table-layout:fixed">
    <tr>
      <td style="width:20px"></td>
      <td tyle="width:120px"> $item</td>
    </tr>
</table>
<table border="0" style="table-layout:fixed">
    <tr>
      <td style="width:60px"></td>
      <td style="width:500px">$paragraph</td>
      <td></td>
    </tr>
</table>

));
}	
1;
