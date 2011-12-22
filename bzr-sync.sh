#!/bin/bash

rev_a=$(bzr revno)
branch=$(git status -sb | grep "##" | cut -d' ' -f2)

git checkout master
bzr pull
rev_b=$(bzr revno)

if [ "${rev_a}" = "${rev_b}" ]; then
	echo No change upstream, aborting.
	git checkout ${branch}
	exit 0
fi

git add -A
git commit -s -m "Updating master to Juju revision ${rev}"
git push origin master

git checkout ${branch}
git rebase master
