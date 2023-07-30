set -e;

cd tilestore; make -j9
cd ../buffertile; make -j9
cd ../tilechunk; make -j9
cd ../linear_algebra_module; make -j9
cd ../lam_executor; make -j9
cd ../evaluation/prevision; rm exec_eval || true;
make -j9
cd ../..;