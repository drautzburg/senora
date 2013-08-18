# --------------------------------------------------------------------------------  
# NAME
#	Spm - Not needed unless you work on the Spm project
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# --------------------------------------------------------------------------------  
# $log$
# --------------------------------------------------------------------------------  

package Spm;
use strict;
use Plugin;
use SessionMgr;
use Text::Wrap;
use Getopt::Long;
use File::Basename;
use Misc;

Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");


use vars qw 
(
 @ISA
 $COMMANDS
 $MRM_PATTERNS
);

@ISA = qw (
	   Plugin
);
# ------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this;

	$this->{commands} = $COMMANDS;

	bless ($this, $class);

	return $this;
}



# ------------------------------------------------------------
$COMMANDS->{spmVersions} =
  {
   alias => "spv",
   description => "show version numbers",
   synopsis => 
   [
    ["pattern"],
   ],
   routine => \&spmVersions
  };

sub spmVersions {
	my ($this, $argv, $opts) = @_;

	my $sql = $Senora::SESSION_MGR->{mainSession};
	my $pattern="%".uc($argv->[0])."%";
	$sql->run("
		select *
		from spm_tb_versions
		where upper(filename) like '$pattern'
	");

}

# ------------------------------------------------------------
$COMMANDS->{domain} =
  {
   alias => "",
   description => "show domain of dve",
   synopsis => 
   [
    ["dve_id"],
    ["-i","id","dom_id or dve_d"],
    ["-n","name","dom_name"],
   ],
   routine => \&domain
  };

sub domain {
	my ($this, $argv, $opts) = @_;
	my $clause;
	$clause .= "and upper(dom_name) like '%".uc($opts->{n})."%'" if $opts->{n};
	$clause .= "and (dve_id = $opts->{i} or dom.dom_id = $opts->{i})" if $opts->{i};

	sql->run("
		select dom.dom_id, dve.dve_id, dom_name, dmt_descr
		from dmt, dom, dve
		where 1=1
		$clause
		and dve.dom_id = dom.dom_id
		and dom.dmt_id = dmt.dmt_id
	");

}

# ------------------------------------------------------------
$COMMANDS->{create_f_get_item_Info} =
  {
   alias => "",
   description => "create Torsten's function",
   synopsis => 
   [
    [],
   ],
   routine => \&create_f_get_item_info
  };

sub create_f_get_item_info {
	my $sql = $Senora::SESSION_MGR->{mainSession};
	$sql->run(qq(
	CREATE OR REPLACE FUNCTION TRA_F_get_item_info
	 (
	   v_aic_id  IN      ADM_V_Address_Item.aic_id%TYPE,
	   v_ait_id  IN      ADM_V_Address_Item.ait_id%TYPE
	 )
	   RETURN VARCHAR2
	   IS

	   l_item_info VARCHAR2 (1024) := NULL;
	BEGIN

	   FOR ait_rec IN
	    (
	      SELECT aic.aic_code    aic_code,
		     aic.aic_seq_num aic_seq_num,
			DECODE
			 (
			   ina.ina_name,
			   NULL,
			   DECODE
			    (
			      ait.ait_num_low  || ait.ait_alpha_low,
			      ait.ait_num_high || ait.ait_alpha_high,
			      ait.ait_num_low  || ait.ait_alpha_low,
			      ait.ait_num_low  || ait.ait_alpha_low  ||'-'||
				ait.ait_num_high || ait.ait_alpha_high||','||ait_num_seq
			    ),
			   ina.ina_name
			 ) || ' (' || aic.aic_code || ')'
		     node_name
		 FROM ADM_V_Item_Name             ina,
		      ADM_V_Has_Address_Name      han,
		      ADM_V_Address_Item          ait,
		      ADM_V_Address_Item_Category aic,
		  (
		      SELECT aic_id, ait_id, LEVEL cai_level
			 FROM ADM_V_Covers_Address_Item
			 START WITH ait_id = v_ait_id
			 AND        aic_id = v_aic_id
			 CONNECT BY PRIOR ait_id_super = ait_id
			 AND        PRIOR aic_id_super = aic_id
		  )   cai
		 WHERE ina.ina_id   (+) = han.ina_id
		 AND   han.anc_id   (+) = 1
		 AND   han.ait_id   (+) = cai.ait_id
		 AND   han.aic_id   (+) = cai.aic_id
		 AND   ait.ait_id       = cai.ait_id
		 AND   ait.aic_id       = cai.aic_id
		 AND   aic.aic_id       = cai.aic_id
		 ORDER BY cai.cai_level DESC
	    )
	   LOOP
	      IF l_item_info IS NOT NULL THEN
		 l_item_info := l_item_info || '; ';
	      END IF;
	      l_item_info := l_item_info || TO_CHAR (ait_rec.aic_seq_num) ||
			     ' ' || ait_rec.node_name;
	   END LOOP;


	   RETURN l_item_info;


	   EXCEPTION
	   WHEN oTHERS THEN
	      RETURN l_item_info || 'error';

	END TRA_F_get_item_info;
	));
}
# ------------------------------------------------------------
$COMMANDS->{generateXud} =
  {
   alias => "",
   description => "generate and XID file",
   synopsis => 
   [
    [],
    ["-f","file_name","the file to generate (default /senora.xud)"],
   ],
   routine => \&generateXud
  };

sub generateXud {
	my ($this, $argv, $opts) = @_;

	my $sql = $Senora::SESSION_MGR->{mainSession};
	my $path = $opts->{f} ? $opts->{f} : "/senora.xud";
	my $theDir = dirname($path);
	my $theFile = basename ($path);

	Bind::declareVar(":ec", 32, 0);
	Bind::declareVar(":em", 256, "okay");
	my $sth=$sql->prepare(qq(
		Begin
			ADM_PA_Xud_Server.generate_xud_file(
				1, '$theDir', '$theFile', :ec, :em
			);
		end;
	), "plsql");
	$sth->execute;
	Bind::printVar(":ec");
	Bind::printVar(":em");
}

# ------------------------------------------------------------
$COMMANDS->{get_unassigned} =
  {
   alias => "gup",
   description => "get unassigned products of domain",
   synopsis => 
   [
    [],
    ["-d", "dve_id", ],
    ["-a", undef, "don\'t stop after 200 records"],
    ["-i", "iif_id", "restrict to import interface"],
    ["-s", "soa_id", "restrict to import interface"],
    ["-n", undef, "only count unassigned products"],
   ],
   routine => \&gup
  };

sub gup {
	my ($this, $argv, $opts) = @_;
	my $limitClause = "and rownum <= 200" unless $opts->{a};
	my $iifClause = "and soa.iif_id = $opts->{i}" if $opts->{i};
	my $soaClauseS = "and soa.soa_id = $opts->{s}" if $opts->{s};
	my $soaClauseC = "and ctp.soa_id = $opts->{s}" if $opts->{s};

	my $countClauseStart;
	my $countClauseEnd;
	if ($opts->{n}) {
		$countClauseStart = "	select soa_id, aic_id, count(*) from (";
		$countClauseEnd = "		) group by soa_id, aic_id";
		$limitClause=undef;
	}


	if (!$opts->{d}) {
		print "You must specify a domain\n";
		return;
	}
	my $dve_id = $opts->{d};
	$this->domain([$dve_id]);

	sql->run(qq(
	$countClauseStart
		select 	/*+ FIRST_ROWS*/
			sop.aic_id,
			sop.dve_id, 
			sop.sop_id, 
			sop.sop_sortcode, 
			sop.iif_id,
			soa.soa_id
		from
			sop,
			soa
		where
			sop.dve_id=$dve_id
		and	soa.dve_id=$dve_id
			and soa.iif_id = sop.iif_id $iifClause $soaClauseS
	      minus
		select 
			sop.aic_id,
			sop.dve_id, 
			sop.sop_id, 
			sop.sop_sortcode, 
			sop.iif_id,
			ctp.soa_id
		from
			sop,
			ctp
		where
			sop.dve_id=$dve_id
		and	sop.sop_id = ctp.sop_id
		and	ctp.dve_id=$dve_id $soaClauseC
	$countClauseEnd
	)); 
}


# ------------------------------------------------------------
$COMMANDS->{periodic_import} =
  {
   alias => "pimp",
   description => 'start, stop or query periodic import',
   synopsis => 
   [
    [],
    ["-1", undef, "start import"],
    ["-0", undef, "abort import"],
    ["-i", undef, "inqury status of import"],
    ["-t", "type", "set import type (ADDR)"],
   ],
   routine => \&periodic_import
  };

sub periodic_import {
	my ($this, $argv, $opts) = @_;

	my $itype= $opts->{t} ? $opts->{t} : "ADDR";
	$itype ="'".$itype."'";

	if ($opts->{1}) {
		print "starting import\n";
		sql->run (qq(
			begin
			adm_pa_import_server_control.start_import ($itype, :ec, :em);
			end;
		),"plsql");
		Bind::printVar('ec');
		Bind::printVar('em');
		return;
	}
	if ($opts->{0}) {
		print "stopping import\n";
		sql->run (qq(
			begin
			adm_pa_import_server_control.abort_import ($itype, :ec, :em);
			end;
		), "plsql");
		Bind::printVar('ec');
		Bind::printVar('em');
		return;
	}
	if ($opts->{i}) {
		my $script=(qq(
 	SELECT
		imt.imt_type_id,
		isn.isn_name,
		imt.imt_num_errors,
		imt.imt_pct_completion,
		ima.ima_name,
		imt.imt_datetime
  	FROM	adm_v_import_status_name isn,
		adm_v_import imt,
		adm_v_import_action ima
	WHERE	isn.ims_id      = imt.imt_status_id
        AND     imt.imt_type_id = ima.imt_type_id
	AND	isn.lan_id      = 'en'
	AND	ima.ima_lan_id  = 'en'
	));
		sql->run($script);

		return;
	}

}

# ------------------------------------------------------------
$COMMANDS->{get_address} =
  {
   alias => "gad",
   description => "get address of sortcode",
   synopsis => 
   [
    [],
    ["-s", "sortcode", "the sortcode to look up"],
    ["-v", undef, "be verbose"],
   ],
   routine => \&gad
  };

sub gad {
	my ($this, $argv, $opts) = @_;
	my $verboseColumns = ",sop.aic_id, sop.ait_id, sop.dve_id, sop.sop_id, sop.sop_sortcode" if $opts->{v};
	sql->run (qq(
		select
			dom_name,
			tra_f_get_item_info (aic_id, ait_id) Address $verboseColumns

		from
			dom,
			dve,
			sop 
		where
			sop.sop_sortcode = '$opts->{"s"}'
		and	sop.dve_id = dve.dve_id
		and	dve.dom_id = dom.dom_id
	));
}

# ------------------------------------------------------------
$COMMANDS->{expand_sortplan} =
  {
   description => "show sorplan with code ranges",
   synopsis => 
   [
    [],
    ["-s", "name", "the name of the sortplan"],
    ["-a", undef, "don't stop after 500 rows"],
    ["-S", "stacker", "only show this stacker"],
    ["-p", "pattern", "only show matching codes"]
   ],
   routine => \&expand_sortplan
  };

sub expand_sortplan {
	my ($this, $argv, $opts) = @_;
	my $codeClause = qq(
			and (
				scr_sortcode_low like  $opts->{p}
				or
			    	scr_sortcode_high like  $opts->{p}
			)
 	) if $opts->{p};
	my $sol_name;
	if (!($sol_name = $opts->{"s"})) {
		print "no sortplan\n";
		return;
	}

	my $stackerClause = qq(
		and pro.pro_name = $opts->{S}
	) if $opts->{S};
	sql->run (qq(
		select dom_name, sol_name, prs_name, sol.dve_id, sol.sol_id
		from sol, vfr, prs, dve, dom
		where sol_name = '$opts->{"s"}'
		and sol.dve_id = vfr.dve_id
		and sol.sol_id = vfr.sol_id
		and vfr.prs_id = prs.prs_id
		and sol.dve_id = dve.dve_id
		and dve.dom_id = dom.dom_id
	));

	my $stmt =qq[
		select 
			pro.pro_name stacker, 
			sop.sop_name ,
			ctp.dve_id,
			ctp.sop_id,
			scr.scr_sortcode_low,
			scr.scr_sortcode_high
		from sol, spl, ctp, sop , scr, oso, pro
		where 
			sol_name='$sol_name'
		and	sol.spl_id = spl.spl_id
		and	sol.dve_id = spl.dve_id
		and	spl.sop_id_super = ctp.sop_id_super
		and	spl.dve_id = ctp.dve_id
		and	spl.soa_id = ctp.soa_id
		and	ctp.sop_id_connect = sop.sop_id
		and 	ctp.dve_id_connect = sop.dve_id
		and	sop.sop_id = scr.sop_id(+)
		and	sop.dve_id = scr.dve_id(+)
		and 	oso.dve_id = sol.dve_id
		and	oso.sol_id = sol.sol_id
		and	oso.sop_id = ctp.sop_id
		and	oso.prs_id = pro.prs_id
		and	oso.pro_seq_num = pro.pro_seq_num
		and	oso.prs_id = (select min (prs_id)
			from vfr
			where vfr.dve_id = oso.dve_id
			and vfr.sol_id = sol.sol_id
	        )
		$stackerClause
		$codeClause
		order by to_number(stacker),scr.scr_sortcode_low
	];
       	my $countClause = "and rownum < 500" unless $opts->{a};

	if ($opts->{a}) {
		sql->run($stmt);
	} else {
		sql->run ("select * from ($stmt) where rownum<500");
	}

}


# ------------------------------------------------------------
$COMMANDS->{spmJournal} =
  {
   alias => "spmj",
   description => "show journal entries",
   synopsis => 
   [
    [],
    ["-p", "pattern", "a pattern 'Fribourg%'"],
    ["-d", "days ago", "single day  (today)"],
    ["-t", "from,to", "two date boundries"],
    ["-S", "s,s,s", "restrict to subsystems"],
    ["-v", undef, "be verbose"],
   ],
   routine => \&spmJournal
  };

sub spmJournal {
	my ($this, $argv, $opts) = @_;
	my $patternClause = qq(
		and (
			uac.uac_id in (
				select uac_id from 
				spm_tb_user_action_parameter
				where upper (uap_name) like upper ('$opts->{p}')
			)
			or
			upper (uat.uan_name) like upper ('$opts->{p}')
		)
	) if $opts->{p};

	my $timeClause = qq(
		and	trunc(uac_date) = trunc(sysdate)
	);
	$timeClause = qq (
		and	trunc(uac_date) = trunc(sysdate) - $opts->{d}
	) if $opts->{d};


	if (my $dates=$opts->{t}) {
		my @dates = split(",",$dates);
		$timeClause = qq (
		and	trunc(uac_date) 
			between to_date('$dates[0]','dd.mm.')
			and to_date('$dates[1]','dd.mm.')
		);
	}
	my $subsystemClause = qq(
		and sbs_code in ($opts->{S})
	) if $opts->{S};

	#$timeClause="";
	my $script = qq[
		select 
			decode(uap.uap_seq_num,1,uac.uac_date,NULL) uac_date,
			decode(uap.uap_seq_num,1,usr.usr_name,NULL) UserName,
			decode(uap.uap_seq_num,1,sbs.sbs_code,NULL) Module,
			decode(uap.uap_seq_num,1,uat.uan_name,'') operation,
			uap.uap_seq_num i,
			uan.upn_name,
			uap.uap_name
		from
			spm_tb_user_action uac,
			spm_tb_user_action_parameter uap,
			spm_tb_action_par_type_name uan,
			spm_tb_user_action_type_name uat,
			spm_tb_subsystem sbs,
			spm_tb_user usr
		where	1=1
		and	uac.uac_id = uap.uac_id
		and	uap.upt_id = uan.upt_id
		and	uac.uat_id = uat.uat_id
		and	uat.lan_id='en'
		and	uan.lan_id='en'
		and	uac.sbs_id = sbs.sbs_id
		and	uac.usr_id = usr.usr_id
		$timeClause
		$patternClause
		$subsystemClause
		and 	(rownum  < 1000) 
		order by uac.uac_id, uap.uap_seq_num
	];
	sql->run($script);
}


# ------------------------------------------------------------
$COMMANDS->{followCode} =
  {
   alias => "foc",
   description => "show products and sortplans containing code",
   synopsis => 
   [
    [],
    ["-c", "code", "the sortcode to follow"],
    ["-I", undef, "show IDs"],
    ["-s", undef, "show sortplans"],
    ["-v", undef, "be verbose"],
    ["-C", "ctr", "restrict by center pattern"],
    ["-M", "machine", "restrict by machine pattern"],
   ],
   routine => \&followCode
  };

sub followCode {
	my ($this, $argv, $opts) = @_;


	my $dveid;
	my $soaid;
	my $sopid;
	my $solid;
	my $iifid;
	my $solColumns;
	my $script;
	if ($opts->{I}) {
		$dveid = "sop.dve_id,";
		$soaid = "soa.soa_id,";
		$sopid = "sop.sop_id,";
		$solid = "oso.sol_id,";
		$iifid = "sop.iif_id,";
	}
	my $centerClause;
	if ($opts->{C}) {
		$centerClause = "and dom.dom_name like '%$opts->{C}%'";
	}

	my $machineClause;
	if ($opts->{M}) {
		$machineClause = "and prs.prs_name like '%$opts->{M}%'";
	}


	if ($opts->{"s"}) {
		if ($opts->{I}) {
			$dveid = "sop.dve_id,";
			$soaid = "soa.soa_id,";
			$sopid = "sop.sop_id,";
		}

		# include sortplan information
		$script = qq (
		select
			hie.l,
			$dveid
			dom.dom_name,
			$soaid
			soa.soa_name,
			$sopid
			$iifid
			sop.sop_name,
			$solid
			sol.sol_name,
			prs.prs_name,
			oso.pro_seq_num stacker
		from
		(
			select level l, dve_id, sop_id, soa_id
			from	ctp
			start with (dve_id, sop_id) = (
				select dve_id, sop_id
				from sop
				where sop_sortcode = '$opts->{c}'
				and rownum < 2
			)
			connect by prior sop_id_super = sop_id_connect
			and	prior dve_id = dve_id_connect
		) hie,
			sop,
			dve,
			dom,
			soa,
			oso,
			sol,
			prs
		where 	sop.dve_id = hie.dve_id
		and 	sop.sop_id = hie.sop_id
		and	sop.dve_id = dve.dve_id
		and	dve.dom_id = dom.dom_id
		and	hie.soa_id = soa.soa_id
		and	hie.dve_id = soa.dve_id
		and	sop.sop_id = oso.sop_id
		and	sop.dve_id = oso.dve_id
		and	sol.sol_id = oso.sol_id
		and	oso.prs_id = prs.prs_id
	        $centerClause $machineClause);

	} else {
		# don't include sortplan information
		$script = qq (
		select
			hie.l,
			$dveid
			dom.dom_name,
			$soaid
			soa.soa_name,
			$sopid
			$iifid
			sop.sop_name
		from
		(
			select distinct level l, dve_id, sop_id, soa_id
			from	ctp
			start with (dve_id, sop_id) = (
				select dve_id, sop_id
				from sop
				where sop_sortcode = '$opts->{c}'
				and rownum < 2
			)
			connect by prior sop_id_super = sop_id_connect
			and	prior dve_id = dve_id_connect
		) hie,
			sop,
			dve,
			dom,
			soa
		where 	sop.dve_id = hie.dve_id
		and 	sop.sop_id = hie.sop_id
		and	sop.dve_id = dve.dve_id
		and	dve.dom_id = dom.dom_id
		and	hie.soa_id = soa.soa_id
		and	hie.dve_id = soa.dve_id
	        $centerClause);
	    }
	sql->run($script);

	my $superiors = sql->runArrayref (qq(
		select 
			soa_id, count(distinct sop_id_super)
		from 
			sop, ctp
		where 
			sop.sop_sortcode = '$opts->{c}'
		and 	sop.dve_id = ctp.dve_id
		and 	sop.sop_id = ctp.sop_id
		group by 
			soa_id
		having 
			count(distinct sop_id_super) > 1
	));
	if ($superiors->[0]->[1]) {
		print "\nFound multiple superiors !\n";
	}

}

# ------------------------------------------------------------
$COMMANDS->{mrmSearch} =
  {
   description => "test mrm search",
   alias => "mrms", 
   synopsis => 
   [
    [],
    ["-c",undef,"clear search patterns"],
    ["-l",undef,"print output in long forma"]
   ],
   routine => \&mrmSearch
  };

sub mrmSearch {
	my ($this, $argv, $opts) = @_;
	my $resultColumns;
	if ($opts->{l}) {
		$resultColumns = '*';
	} else {
		$resultColumns = qq(
		SVE_ID         ,
		SVE_ORDER_NO   ,
		SVE_IN_ADDR_DIR siad,
		SVE_ACTIVE     ,
		CUS_NAME       ,
		CUS_FIRST_NAME ,
		POSTCODE       ,
		CITY           ,
		STREET_HNR     ,
		POST_OFFICE    
	);
	}

	$MRM_PATTERNS = 
	  [
	   ['ONR','ONR','Order no', undef],
	   ['CUS','LNM','Customer name', undef],
	   ['PCO','PCO','Postcode', undef],
	   ['ADR','STR','Street', undef],
	   ['ADR','CTY','City', undef],
	   ['ADR','POF','Post office', undef]
	   ] if $opts->{c} || ! $MRM_PATTERNS;

	foreach my $p(@$MRM_PATTERNS) {
		printf ("%-16s (%-10s):", $p->[2], $p->[3]);
		my $answer = <>;
		chomp ($answer);
		$p->[3] = $answer if (length($answer) > 0);
		$p->[3] = undef if $answer eq '-';
	}

	sql->run ("delete from mrm_tb_ifc_search_pattern");
	foreach my $p(@$MRM_PATTERNS) {
		if ($p->[3]) {
		sql->run ("insert into mrm_tb_ifc_search_pattern
			values ('$p->[0]','$p->[1]','$p->[3]')");
	}}

	sql->run ("select * from mrm_tb_ifc_search_pattern");
	sql->run ("insert into mrm_tb_ifc_search_is_ready
			values (1,0)");

	sql->run("select $resultColumns from mrm_tb_ifc_search_result");

}

# ------------------------------------------------------------
$COMMANDS->{getSuperiors} =
  {
   description => "get superios sops",
   alias => "gsup", 
   synopsis => 
   [
    [],
    ["-c","code","sortcode"],
   ],
   routine => \&getSuperiors
  };

sub getSuperiors {
	my ($this, $argv, $opts) = @_;
	return unless $opts->{c};

	sql->run(qq(
		select 
			distinct 
				dom_name,
				soa_name,
				sup.dve_id, 
				sup.sop_id,
				sup.sop_name
		from 
			dom,
			dve,
			soa,
			sop sub,
			ctp,
			sop sup
		where 
			sub.sop_sortcode = '$opts->{c}'
		and	ctp.dve_id = sub.dve_id
		and	ctp.sop_id = sub.sop_id
		and	ctp.sop_id_super = sup.sop_id
		and	ctp.dve_id = sup.dve_id
		and	sup.dve_id = dve.dve_id
		and	dve.dom_id = dom.dom_id
		and	ctp.soa_id = soa.soa_id
	));
}




# ------------------------------------------------------------
$COMMANDS->{checkDeliveryDomain} =
  {
   description => "check for overlapping sortcodes",
   alias => "cdd", 
   synopsis => 
   [
    [],
    ["-p","pattern","Domain name patern using %"],
    ["-q",undef,"quiet: only print bad domains"],
    ["-s",undef,"print superior information"]
   ],
   routine => \&checkDeliveryDomain
  };

sub checkDeliveryDomain {
	my ($this, $argv, $opts) = @_;

	my $pattern = $opts->{p} ? $opts->{p} : '%';

	my $domains = sql->runArrayref (qq(
		select 
			dom_name, dom.dom_id, dve_id
		from 
			dom, dve
		where 
			dom.dom_id = dve.dom_id
		and	dom.dmt_id = 3
		and	upper(dom.dom_name) like  upper('$pattern')
	));

	# print header
	my $nOffices = $#$domains + 1;
	if ($nOffices > 0) {
		print "Checking $nOffices offices\n\n";
		printf ("%-40s %-12s %s\n",
			"Delivery office","dve_id","ok ?");
		print "---------------------------------";
		print "---------------------------------\n";
	}

	foreach my $d (@$domains) {

		my $result = sql->runArrayref (qq(
	 		select /*+ RULE*/ sop.sop_sortcode
           		from
                		sop,
                		ctp
           		where
                		sop.dve_id = ctp.dve_id
           			and  sop.sop_id = ctp.sop_id
           			and  ctp.dve_id = $d->[2]
           			and  sop.sop_sortcode is not null
           		group by
                		sop.sop_sortcode, ctp.soa_id
           		having
                		count (distinct ctp.sop_id_super) > 1
           		));
		if ($#$result == -1) {
			unless ($opts->{"q"}) {
				printf "%-40s %-12d", $d->[0], $d->[1];
				print " no overlaps\n";
				}
		} else {
			printf "%-40s %-12d", $d->[0], $d->[1];
			print " bad codes\n";
			foreach my $s(@$result ) {
				printf ("%s\n", $s->[0]);
				# maybe print superiors
				if ($opts->{"s"}) {
					$this->getSuperiors (undef, {c => $s->[0]});
					print "\n==================\n";
				}
			}
		}
	}
}

# ------------------------------------------------------------
$COMMANDS->{sortingLists} =
  {
   description => "display status of sorting lists",
   alias => "slo", 
   synopsis => 
   [
    [],
    ["-D","domain","Domain pattern (needs %)"],
    ["-S","sol_name","Sorting Linst name pattern (needs %)"],
    ["-a",undef,"show entrire history not just last entry"],
    ["-i",undef,"show unsuccessful entries only"],
    ["-I", undef, "show IDs"],
   ],
   routine => \&sortingLists
  };

sub sortingLists {
	my ($this, $argv, $opts) = @_;

	my $maxClause = qq(
		and	slo_id = (
			select max(slo_id) 
			from slo
			where slo.dve_id=ss.dve_id
			and slo.sol_id =ss.sol_id
		)) unless $opts->{a};

	my $idClause = qq(
		ss.dve_id, 
		ss.sol_id, 
	) if $opts->{I};

	my $domainClause = "
		and upper(dom.dom_name) like '".uc($opts->{D})."'
	" if $opts->{D};

	my $solnameClause = "
		and upper(sol.sol_name) like '".uc($opts->{S})."'
	" if $opts->{S};

	my $successClause = qq(
		and ss.slo_state != 'S'
	) if $opts->{i};

	sql->run (qq(
		select 
			$idClause
			dom.dom_name, 
			sol.sol_name, 
			slo_start_date, 
			slo_type_name ||'-'|| slo_state_name status
		from 
			dve,
			dom,
			sol,
			SPM_V_Sorting_State ss
		where 
			ss.dve_id = dve.dve_id
		and	dve.dom_id = dom.dom_id
		and	ss.dve_id = sol.dve_id
		and	ss.sol_id = sol.sol_id
		and	lan_id='en'
		$maxClause
		$domainClause
		$solnameClause
		$successClause
		order by ss.dve_id, ss.sol_id, ss.slo_id
	));
}

1;

# ------------------------------------------------------------
$COMMANDS->{fillScrInitially} =
  {
   alias => "",
   description => "call fill_scr_initially",
   synopsis => 
   [
    [],
    ],
   routine => \&fillScrInitially,
   };

sub fillScrInitially {
	my ($this, $argv, $opts) = @_;

	sql->run("truncate table spm_tb_changed_item");
	sql->run("truncate table spm_tb_affects_sortcode_range");
	sql->run("truncate table spm_tb_sortcode_range");
	Bind::declareVar(":ec",16,'99');
	Bind::declareVar(":em",256,'unset');
	sql->run(qq(
		begin
		spm_pa_sortcode_range.fill_scr_initially(:ec, :em);
		end;
	), "plsql");
	Bind::printVar("ec");
	Bind::printVar("em");
}

# ------------------------------------------------------------
$COMMANDS->{showAdmTree} =
  {
   alias => "",
   description => "print ADM categories",
   synopsis => 
   [
    [],
    ["-C", "ait_id", "start with this aic_id instead of 1"],
    ["-L", "lan_id", "use this lan_id insead of 'de'"],
    ["-D", "maxdepth", "limit the tree depts"],
    ["-r", undef,    "print reversed tree"]
    ],
   routine => \&printAdmTree,
   };

sub categoryNames {
	my($lanId) = @_;
	my $categories={};
	my $sth = sql->prepare(qq(
		select aic_id, acn_name
		from acn
		where lan_id = '$lanId'
	));
	$sth->execute();
	while (my $row = $sth->fetchrow_arrayref) {
		$categories->{$row->[0]} = $row->[1];
	}
	return $categories;
}

sub nSpaces{
	my ($spaces) = @_;
	#print ("xx $spaces\n");
	return if ($spaces == 0);
	return " ".nSpaces($spaces-1);
}

sub printAdmTree {
	my ($this, $argv, $opts) = @_;
	my $aicId = $opts->{C} ? $opts->{C} : 1;
	my $lanId = $opts->{L} ? $opts->{L} : 'de';

	my $categories = categoryNames($lanId);

	my $sth;
	if ($opts->{r}) {
		$sth = sql->prepare(qq(
			select level, aic_id 
			from adm_tb_covers_address_item_cat 
			start with aic_id=$aicId
			connect by prior aic_id_super = aic_id
		));
	}else {
		$sth = sql->prepare(qq(
			select level, aic_id 
			from adm_tb_covers_address_item_cat 
			start with aic_id_super=$aicId
			connect by prior aic_id = aic_id_super
	));
	}
	$sth->execute();

	print "$aicId $categories->{$aicId} \n" unless $opts->{r};
	while (my $row = $sth->fetchrow_arrayref) {
		# print respecting maxdepth
		if (!$opts->{D} or $row->[0] < $opts->{D}) {
			my $name = $categories->{$row->[1]};
			print (nSpaces(4*$row->[0]). "$row->[1] " . $name."\n");
		}
	}
}

# ------------------------------------------------------------
$COMMANDS->{showSubordinateProducts} =
  {
   alias => "gsub",
   description => "print ADM categories",
   synopsis => 
   [
    [],
    ["-P", "ProductName", "start with this product"],
    ["-d", "dve_id", "the dve_id of the superior"],
    ["-s", "sop_id", "the sop_id of the superior"],
    ],
   routine => \&showSubordinateProducts,
   };

sub showSubordinateProducts {
	my ($this, $argv, $opts) = @_;
	my $dveId = $opts->{d};
	my $sopId = $opts->{"s"};
	my $productName = $opts->{P};

	if ($dveId && !$sopId) {
		print ("Specify ProductName or sop_id\n");
		return;
	}
	my $superClause="";
	if ($productName) {
		$superClause = qq(and super.sop_name=$productName);
	}else {
		$superClause .= qq (and super.dve_id=$dveId) if $dveId;
		$superClause .= qq (and super.sop_id=$sopId);
	}

	sql->run(qq(
	select /*+ rule */
		super.sop_name superior,
		super.eif_id,
		super.dve_id,
		super.sop_id,
		ctp.dve_id_connect,
		ctp.sop_id_connect,
		sub.sop_name subordinate,
		sub.dve_id,
		sub.sop_id,
		sub.iif_id,
		sub.ait_id,
		sub.aic_id,
		sub.sop_sortcode
	from 
		sop super,
		ctp,
		sop sub
	where   1=1
	$superClause
	and	super.dve_id = ctp.dve_id
	and	super.sop_id = ctp.sop_id_super
	and	sub.dve_id = ctp.dve_id
	and	sub.sop_id = ctp.sop_id
	));
}

# ------------------------------------------------------------
$COMMANDS->{check_dve_id_connect} =
  {
   description => "check for bad dve_id/sop_id_connect",
   synopsis => 
   [
    [],
   ],
   routine => \&check_dve_id_connect,
   };

sub check_dve_id_connect {
	my ($this, $argv, $opts) = @_;

	print "Checking across contratcs. Please wait...\n";
	sql->run(qq(
	select
		sop1.dve_id,
		sop1.sop_id,
		ctp.dve_id_connect,
		ctp.sop_id_connect
	from
		sop sop1, 
		ctp
	where
		ctp.dve_id = sop1.dve_id
	and	ctp.sop_id = sop1.sop_id
	and	sop1.iif_id is not null
	and	sop1.sop_sortcode is null
	and not exists (
		select 1 
		from sop sop2
		where 
			sop2.eif_id is not null
		and 	sop2.cop_id = sop1.cop_id
		and 	sop2.cpl_id = sop1.cpl_id
		and 	sop2.cve_id = sop1.cve_id
	)
	));

	print "Checking private intermediates. Please wait...\n";

	sql->run(qq(
	select
		sop.dve_id,
		sop.sop_id,
		ctp.dve_id_connect,
		ctp.sop_id_connect
	from
		sop,
		ctp
	where
		ctp.dve_id = sop.dve_id
	and	ctp.sop_id = sop.sop_id
	and	sop.iif_id is  null
	and	nvl(ctp.dve_id_connect,0) != sop.dve_id
	and	nvl(ctp.sop_id_connect,0) != sop.sop_id
	));
}

# ------------------------------------------------------------
$COMMANDS->{spmComputers} =
  {
   description => "check for bad dve_id/sop_id_connect",
   synopsis => 
   [
    [],
    ["-l", undef, "print long format"],
    ["-i", "id", "print computers with given id"]
   ],
   routine => \&spmComputers
   };

sub spmComputers {
	my ($this, $argv, $opts) = @_;

	my $longColumns ="";
	$longColumns = qq(
			,
			COM_NET_ADDRESS net_address,
			COM_OPER_SYSTEM oper_system,
			COM_ACCOUNT_NAME account_name,
			COM_ACCOUNT_PASSWORD account_password

	) if $opts->{l};
	my $query = qq(
		select 
			plo_name,
			com_name,
			crn.crn_name
			$longColumns
		from
			spm_tb_proc_location plo,
			spm_tb_computer com,
			Spm_Tb_Plays_Computer_Role pcr,
			Spm_Tb_Computer_Role_Name crn
		where	1=1
		and	com.plo_id = plo.plo_id
		and	com.com_id = pcr.com_id
		and	pcr.cor_id = crn.cor_id
		and	crn.lan_id='en'

	);

	sql->run($query);
}


1;
