::#######################################################################################################################
:: 
::  Copyright (C) 2010 by the Aura project (http://aura.eu)
:: 
::  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
::  the License. You may obtain a copy of the License at
:: 
::      http://www.apache.org/licenses/LICENSE-2.0
:: 
::  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
::  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
::  specific language governing permissions and limitations under the License.
:: 
::#######################################################################################################################
setlocal

SET bin=%~dp0
SET AURA_ROOT_DIR=%bin%..
SET AURA_LIB_DIR=%AURA_ROOT_DIR%\lib
SET AURA_CONF_DIR=%AURA_ROOT_DIR%\conf
SET AURA_LOG_DIR=%AURA_ROOT_DIR%\log

SET JVM_ARGS=-Xms768m -Xmx768m

SET AURA_WM_CLASSPATH=%AURA_LIB_DIR%\*

java %JVM_ARGS% -cp "%AURA_WM_CLASSPATH%" de.tuberlin.aura.workloadmanager.WorkloadManager -executionMode local -configDir "%AURA_CONF_DIR%"

endlocal
