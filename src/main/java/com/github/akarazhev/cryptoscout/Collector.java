/*
 * MIT License
 *
 * Copyright (c) 2025 Andrey Karazhev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.akarazhev.cryptoscout;

import com.github.akarazhev.cryptoscout.module.CollectorModule;
import com.github.akarazhev.cryptoscout.module.CoreModule;
import com.github.akarazhev.cryptoscout.module.WebModule;
import io.activej.inject.module.Module;
import io.activej.jmx.JmxModule;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import static io.activej.inject.module.Modules.combine;

public final class Collector extends Launcher {

    @Override
    protected Module getModule() {
        return combine(
                JmxModule.create(),
                ServiceGraphModule.create(),
                CoreModule.create(),
                CollectorModule.create(),
                WebModule.create());
    }

    @Override
    protected void run() throws Exception {
        awaitShutdown();
    }

    public static void main(final String[] args) throws Exception {
        new Collector().launch(args);
    }
}
