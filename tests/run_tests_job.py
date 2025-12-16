import pytest
import sys
import os

def run_tests():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    tests_dir = os.path.dirname(current_dir)
    
    project_root = os.path.dirname(tests_dir)
    
    print(f"DEBUG: Script rodando em: {current_dir}")
    print(f"DEBUG: Raiz do projeto definida como: {project_root}")
    
    if os.path.exists(project_root):
        print(f"DEBUG: Conteúdo da raiz: {os.listdir(project_root)}")
    else:
        print("DEBUG: ERRO CRÍTICO - Raiz não encontrada!")

    sys.path.insert(0, project_root)
    
    os.chdir(project_root)

    retcode = pytest.main(["-v", "tests/"])
    
    return retcode

if __name__ == "__main__":
    sys.exit(run_tests())