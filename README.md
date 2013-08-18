senora
======

Senora is targeted to be a drop-in replacement for sqlplus. It provides a plethora of new commands, which accept Unix-style options. It is enhanceable via plug-ins, and provides most of sqlplus' functionality.

Senora intends to be your primary Oracle shell. While there are may nifty Oracle tools around (e.g. TORA or TOAD), these do not attempt to replace sqlplus, so chances are you'll need to run sqlplus additionally. Also these tools are hard (TORA) or impossible (TOAD, sqlplus) to extend. Senora attempts to give you much of the browsing and anayzing capabilities of these tools without the need to run any other tool. 

You can extend Senora easily by providing you own plugins. In fact most of Senora's core functionality is written as plugins. A plugin provides additional commands to Senora and integrates these with Senora's help system. Senora's extensions are thus self-documenting as we know it from emacs.

This is just one benefit of plugins over stand-alone sql-scripts. Another one is the ability to provide unix-style options. The lack of options in sqlplus was in fact the main reason I started to write senora. Options allows you to group similar commands under one common hood, even when the undelying SQL differs significantly depending on the currently set options. The good thing about this is: you need to remeber less, and if things do slip your memory, the help system will guide you - much faster than any menu driven system ever will. 

Some Plugins have grown into extremely useful tools. I am getting a reputation for being an oracle wizard, just becuase I fire up the Senora's profiler (from the Tuning plugin) whenver someone ask the question "why is this so slow ?"

Senora attempts to provide a friendlier output formatting than sqlplus. Columns tend to be only as wide a really needed, and when linebreaks are needed, these are placed at "good" positions, i.e. after blanks or undescores. This is of course an incompatibility to sqlplus, and programs that rely on sqlplus-style fromatting may not work with Senora. Still Senora understands some of sqlpluses columns formatting commands.

Senora can run legacy sqlplus scripts as long as these do non use any unsupported commands. The most important sqlplus commands however do work, including starting scripts from within a script (@ and @@), using defines (see ampersand replacing) and bind variables.
 
	
