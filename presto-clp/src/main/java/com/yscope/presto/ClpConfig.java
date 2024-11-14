/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yscope.presto;

import com.facebook.airlift.configuration.Config;

public class ClpConfig
{
    private String clpArchiveDir;
    private boolean polymorphicTypeEnabled;
    private InputSource inputSource = InputSource.FILESYSTEM;

    public String getClpArchiveDir()
    {
        return clpArchiveDir;
    }

    @Config("archive-dir")
    public ClpConfig setClpArchiveDir(String clpArchiveDir)
    {
        this.clpArchiveDir = clpArchiveDir;
        return this;
    }

    public boolean isPolymorphicTypeEnabled()
    {
        return polymorphicTypeEnabled;
    }

    @Config("polymorphic-type-enabled")
    public ClpConfig setPolymorphicTypeEnabled(boolean polymorphicTypeEnabled)
    {
        this.polymorphicTypeEnabled = polymorphicTypeEnabled;
        return this;
    }

    public InputSource getInputSource()
    {
        return inputSource;
    }

    @Config("input-source")
    public ClpConfig setInputSource(InputSource inputSource)
    {
        this.inputSource = inputSource;
        return this;
    }

    public enum InputSource
    {
        FILESYSTEM,
        S3,
        NETWORK,
        TERRABLOB,
    }
}
